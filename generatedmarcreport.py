#!/usr/bin/python
#
#
# Author    Nuno Martins
# Created   2022
# 
# Considerations:
#  1. Shared Mailbox dedicated to receive DMARC Aggregated reports from external entities (Mailbox can receive reports to multiple domains)
#  2. Script use IMAP access to that mailbox 
#  3. Script invoked daily 
#  4. Script send aggregated report for one domain by email (EWS)
#
# An python script that
#  1. Dumps attachs from emails on a given mailbox to a temporary folder received on the last day.  
#  2. Unzip attachs if attach is not .xml 
#  3. Create a struture with all information collect 
#  4. Generate report information for a requested domain (global metrics, top senders and details)
#  5. Send email with report
#

import sys
import os
import socket
import ssl
import imaplib, email
import argparse
import getpass
import datetime
from datetime import datetime, timedelta
import zipfile
import gzip
import shutil
import xml.etree.cElementTree as etree
import socket
import zipfile
from collections import defaultdict
from exchangelib import Credentials, Configuration, Account, Message, Mailbox, FileAttachment, DELEGATE, HTMLBody
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
import json
import pandas as pd
import io


##########################################################################################
# Override IMAP4_SSL to validate server identity (CA cert validation)
class IMAP4_SSL_Ex(imaplib.IMAP4_SSL):
  def __init__(self, host = '', port = imaplib.IMAP4_SSL_PORT,
                                ca_certs = None, cert_reqs = ssl.CERT_REQUIRED,
                                ssl_version = ssl.PROTOCOL_TLSv1):
    self.cert_reqs = cert_reqs
    self.ca_certs = ca_certs
    self.ssl_version = ssl_version
    imaplib.IMAP4_SSL.__init__(self, host, port, keyfile = None, certfile = None)

  def open(self, host = '', port = imaplib.IMAP4_SSL_PORT):
    self.host = host
    self.port = port
    self.sock = socket.create_connection((host, port))
    self.sslobj = ssl.wrap_socket(self.sock, self.keyfile, 
                          self.certfile, 
                          cert_reqs=self.cert_reqs,
                          ssl_version=self.ssl_version,
                          ca_certs=self.ca_certs)
    self.file = self.sslobj.makefile('rb')

##########################################################################################
# Configuration options

args = ""

# Temporary folder used to collect .xml files (dedicated folder - files created and removed)
tempdir = "."

# Will collect only attachs from emails
attachmentsonly = True

# IMAP access to shared mailbox (user1 can read email from shared mailbox)
server = "webmail.contoso.com"
user = "user1@contoso.com\dmarc@contoso.com"
password = "user1password"
mailboxfolder = "INBOX"

# Filter DMARC Aggregated Reports received on the last day
reportdate = (datetime.now() - timedelta(days=1)).strftime('%d-%b-%Y')
searchcriteria = "ON " + reportdate + " SUBJECT \"Report Domain\""

# EWS configuration used to send report 
userad = "CONTOSO\\user1"
ews_url = 'https://webmail.contoso.com/EWS/Exchange.asmx'
primary_smtp_address = 'user1@contoso.com'
destinationaddress = 'soc@contoso.com'

# Default domain to consider on report generation
report_domain = 'contoso.com'

##########################################################################################

def un_zipFile(file,path):
  if file.endswith('.zip'):
    zip_file = zipfile.ZipFile(file)
    for names in zip_file.namelist():
        zip_file.extract(names,path)
        zip_file.close() 
  if file.endswith('.gz'):
    with gzip.open(file, 'rb') as f_in:
      with open(file+'.xml', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    f_in.close()
    f_out.close()
    
def vprint(msg):
  global args
  if args.quiet: return
  if args.verbose: print(msg);

def timestamp_to_datetime(timestamp):
    """
    Converts a UNIX/DMARC timestamp to a Python ``DateTime`` object

    Args:
        timestamp (int): The timestamp

    Returns:
        DateTime: The converted timestamp as a Python ``DateTime`` object
    """
    return datetime.fromtimestamp(int(timestamp))
  
def timestamp_to_human(timestamp):
    """
    Converts a UNIX/DMARC timestamp to a human-readable string

    Args:
        timestamp: The timestamp

    Returns:
        str: The converted timestamp in ``YYYY-MM-DD HH:MM:SS`` format
    """
    return timestamp_to_datetime(timestamp).strftime("%Y-%m-%d %H:%M:%S")

# Dumps emails/attachments in the mailbox to output directory.
def process_mailbox(mail):
  global searchcriteria
  global tempdir
  global attachmentonly
  
  count=0

  ret, data = mail.search(None, '(' + searchcriteria + ')')
  if ret != 'OK':
    print >> sys.stderr, "ERROR: No messages found"
    return 1

  if not os.path.exists(tempdir):
    os.makedirs(tempdir)

  for num in data[0].split():
    ret, data = mail.fetch(num, '(RFC822)')
    if ret != 'OK':
      print >> sys.stderr, "ERROR getting message from IMAP server", num
      return 1
    count = count + 1

    if not attachmentsonly:
      fp = open('%s\%s.eml' %(tempdir, num), 'wb')
      fp.write(data[0][1])
      fp.close()
      count = count + 1
      print(" " + tempdir + "\\" + str(num) + ".eml")
    else:
      raw_email = data[0][1]
      try :
        m =  email.message_from_string(raw_email)
      except  TypeError :
        m =  email.message_from_bytes(raw_email)
      for part in m.walk():
        #find the attachment part
        if part.get_content_type() == 'application/zip' or \
        part.get_content_type() == 'application/gzip': 

        #save the attachment in the given directory
          filename = part.get_filename()
          if not filename: continue
          filename = tempdir+"/"+filename
          fp = open(filename, 'wb')
          fp.write(part.get_payload(decode=True)  )
          fp.close()
          print(filename);
          un_zipFile(filename,tempdir);
          os.remove(filename);
          count = count + 1
    
  if attachmentsonly:
    print("\nTotal attachments downloaded: ", count);
  else:
    print("\nTotal mails downloaded: ", count);

# Acces mailbox where DMARC Aggregated reports are and dumps attachs
def getdmarcreports():
  global password
  global server
  global user
  global mailboxfolder
  
  if args.pwdfile:
    infile = open(args.pwdfile, 'r')
    firstline = infile.readline().strip()
    password = firstline
  else:
    password = getpass.getpass()  

  mail = imaplib.IMAP4_SSL(server);
  mail.login(user, password)

  print("After login ..." );
  ret, data = mail.select(mailboxfolder, True)
  if ret == 'OK':
    print("Processing mailbox: " + mailboxfolder);
    if process_mailbox(mail):
      mail.close()
      mail.logout()
      sys.exit(1)
      
    mail.close()
  else:
    print >> sys.stderr, "ERROR: Unable to open mailbox ", rv
    mail.logout()
    sys.exit(1)

  mail.logout() 

# returns meta fields
def get_meta(context):
  report_meta = ""
  feedback_pub = ""

  pp = 0
  rm = 0  

  for event, elem in context:
    if event == "end" and elem.tag == "report_metadata":
      # process record elements
      org_name = (elem.findtext("org_name", 'NULL')).translate(str.maketrans('','',','))
      email = (elem.findtext("email", 'NULL')).translate(str.maketrans('','',','))
      extra_contact_info = (elem.findtext("extra_contact_info", 'NULL')).translate(str.maketrans('','',','))
      report_id = (elem.findtext("report_id", 'NULL')).translate(str.maketrans('','',','))
      date_range_begin = (elem.findtext("date_range/begin", 'NULL')).translate(str.maketrans('','',','))
      date_range_end = (elem.findtext("date_range/end", 'NULL')).translate(str.maketrans('','',','))
      report_meta =  org_name + "," + timestamp_to_human(date_range_begin) + "," + timestamp_to_human(date_range_end)
      rm = 1
      continue

    if event == "end" and elem.tag == "policy_published":
      domain = elem.findtext("domain", 'NULL')
      adkim = elem.findtext("adkim", 'NULL')
      aspf = elem.findtext("aspf", 'NULL')
      p = elem.findtext("p", 'NULL')
      pct = elem.findtext("pct", 'NULL')
      feedback_pub = domain + "," + p
      pp = 1
      continue      

    if pp == 1 and rm == 1:
      meta = report_meta + "," + feedback_pub
      
      return meta
  
  return

def print_record(context, meta, args):

  report_map = defaultdict()
  
  for event, elem in context:
    if event == "end" and elem.tag == "record":
      source_ip = "";
      count = "";
      disposition = "";
      dkim = "";
      spf = "";
      reason_type = "";
      comment = "";
      envelope_to = "";
      header_from = "";
      dkim_domain = "";
      dkim_result = "";
      dkim_hresult = "";
      spf_domain = "";
      spf_result = "";
      
      # process record elements
      # NOTE: This may require additional input validation
      source_ip = (elem.findtext("row/source_ip", 'NULL')).translate(str.maketrans('','',','))
      count = (elem.findtext("row/count", 'NULL')).translate(str.maketrans('','',','))
      disposition = (elem.findtext("row/policy_evaluated/disposition", 'NULL')).translate(str.maketrans('','',','))
      dkim = (elem.findtext("row/policy_evaluated/dkim", 'NULL')).translate(str.maketrans('','',','))
      spf = (elem.findtext("row/policy_evaluated/spf", 'NULL')).translate(str.maketrans('','',','))
      reason_type = (elem.findtext("row/policy_evaluated/reason/type", 'NULL')).translate(str.maketrans('','',','))
      comment = (elem.findtext("row/policy_evaluated/reason/comment", 'NULL')).translate(str.maketrans('','',','))
      envelope_to = (elem.findtext("identifiers/envelope_to", 'NULL')).translate(str.maketrans('','',','))
      header_from = (elem.findtext("identifiers/header_from", 'NULL')).translate(str.maketrans('','',','))
      dkim_domain = (elem.findtext("auth_results/dkim/domain", 'NULL')).translate(str.maketrans('','',','))
      dkim_result = (elem.findtext("auth_results/dkim/result", 'NULL')).translate(str.maketrans('','',','))
      dkim_hresult = (elem.findtext("auth_results/dkim/human_result", 'NULL')).translate(str.maketrans('','',','))
      spf_domain = (elem.findtext("auth_results/spf/domain", 'NULL')).translate(str.maketrans('','',','))
      spf_result = (elem.findtext("auth_results/spf/result", 'NULL')).translate(str.maketrans('','',','))

      # If you can identify internal IP
      x_host_name = "NULL"
      #try:
      #  if IS_INTERNAL_IP(source_ip):
      #    x_host_name = socket.getfqdn(source_ip)
      #except: 
      #  x_host_name = "NULL"

      # dmarc
      x_dmarcpass = "0";
      x_dmarcfail = "0";
      if (dkim == "pass" or spf == "pass"):
          x_dmarcpass = count;
      else:
          x_dmarcfail = count;
      if int(count) > 0:
          x_dmarcrate = str((int(x_dmarcpass)/int(count))*100) + "%";
      else:
          x_dmarcrate = "0%"

      # spf
      x_spfauthpass = "0";
      x_spfauthfail = "0";
      x_spfalignpass = "0";
      x_spfalignfail = "0";
      if (spf_result == "pass"):
          x_spfauthpass = count;
      if (spf_result == "fail"):
          x_spfauthfail = count;
      if (spf_domain == header_from):
          x_spfalignpass = count;
      else:
          x_spfalignfail = count;
      x_spfpolicypass = x_spfalignpass;


      # dkim
      x_dkimauthpass = "0";
      x_dkimauthfail = "0";
      x_dkimalignpass = "0";
      x_dkimalignfail = "0";
      if (dkim_result == "pass"):
          x_dkimauthpass = count;
      if (dkim_result == "fail"):
          x_dkimauthfail = count;
      if (dkim_domain == header_from):
          x_dkimalignpass = count;
      else:
          if (dkim_domain != 'NULL'):
            x_dkimalignfail = count;
      x_dkimpolicypass = x_dkimalignpass;
      
      if str(source_ip) in report_map:
        report_map[str(source_ip)]["count"] = report_map[str(source_ip)]["count"] + int(count);
        report_map[str(source_ip)]["x_dmarcpass"] = report_map[str(source_ip)]["x_dmarcpass"] + int(x_dmarcpass);
        report_map[str(source_ip)]["x_dmarcfail"] = report_map[str(source_ip)]["x_dmarcfail"] + int(x_dmarcfail);
        report_map[str(source_ip)]["x_spfauthpass"] = report_map[str(source_ip)]["x_spfauthpass"] + int(x_spfauthpass);
        report_map[str(source_ip)]["x_spfauthfail"] = report_map[str(source_ip)]["x_spfauthfail"] + int(x_spfauthfail);
        report_map[str(source_ip)]["x_spfalignpass"] = report_map[str(source_ip)]["x_spfalignpass"] + int(x_spfalignpass);
        report_map[str(source_ip)]["x_spfalignfail"] = report_map[str(source_ip)]["x_spfalignfail"] + int(x_spfalignfail);
        report_map[str(source_ip)]["x_spfpolicypass"] = report_map[str(source_ip)]["x_spfpolicypass"] + int(x_spfpolicypass);
        report_map[str(source_ip)]["x_dkimauthpass"] = report_map[str(source_ip)]["x_dkimauthpass"] + int(x_dkimauthpass);
        report_map[str(source_ip)]["x_dkimauthfail"] = report_map[str(source_ip)]["x_dkimauthfail"] + int(x_dkimauthfail);
        report_map[str(source_ip)]["x_dkimalignpass"] = report_map[str(source_ip)]["x_dkimalignpass"] + int(x_dkimalignpass);
        report_map[str(source_ip)]["x_dkimalignfail"] = report_map[str(source_ip)]["x_dkimalignfail"] + int(x_dkimalignfail);
        report_map[str(source_ip)]["x_dkimpolicypass"] = report_map[str(source_ip)]["x_dkimpolicypass"] + int(x_dkimpolicypass);
      else:
        report_row = defaultdict();
        report_row["count"] = int(count);
        report_row["x_dmarcpass"] = int(x_dmarcpass);
        report_row["x_dmarcfail"] = int(x_dmarcfail);
        report_row["x_spfauthpass"] = int(x_spfauthpass);
        report_row["x_spfauthfail"] = int(x_spfauthfail);
        report_row["x_spfalignpass"] = int(x_spfalignpass);
        report_row["x_spfalignfail"] = int(x_spfalignfail);
        report_row["x_spfpolicypass"] = int(x_spfpolicypass);
        report_row["x_dkimauthpass"] = int(x_dkimauthpass);
        report_row["x_dkimauthfail"] = int(x_dkimauthfail);
        report_row["x_dkimalignpass"] = int(x_dkimalignpass);
        report_row["x_dkimalignfail"] = int(x_dkimalignfail);
        report_row["x_dkimpolicypass"] = int(x_dkimpolicypass);
        report_map[str(source_ip)] = report_row;

      continue

  return report_map;

def print_dmarc_map(dmarc_map):
  print("Organization,Date.Begin,Date.End,Domain,Policy,SourceIP,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Auth.Pass,SPF.Auth.Fail,SPF.Align.Pass,SPF.Align.Fail,SPF.Policy.Pass,DKIM.Auth.Pass,DKIM.Auth.Fail,DKIM.Align.Pass,DKIM.Align.Fail,DKIM.Policy.Pass");
  
  for org in dmarc_map:
    for domain in dmarc_map[org]:
        header = org + "," + dmarc_map[org][domain][0] + "," + dmarc_map[org][domain][1] + "," + domain + "," + \
        dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
          print(header + "," + str(key) + "," + str(report_map[str(key)]["count"]) + "," + str(report_map[str(key)]["x_dmarcpass"]) + "," \
           + str(report_map[str(key)]["x_dmarcfail"]) + "," + str(int((report_map[str(key)]["x_dmarcpass"]/report_map[str(key)]["count"])*100)) + "%" + "," \
           + str(report_map[str(key)]["x_spfauthpass"]) + "," + str(report_map[str(key)]["x_spfauthfail"]) + "," \
           + str(report_map[str(key)]["x_spfalignpass"]) + "," + str(report_map[str(key)]["x_spfalignfail"]) + "," \
           + str(report_map[str(key)]["x_spfpolicypass"]) + "," + str(report_map[str(key)]["x_dkimauthpass"]) + "," \
           + str(report_map[str(key)]["x_dkimauthfail"]) + "," + str(report_map[str(key)]["x_dkimalignpass"]) + "," \
           + str(report_map[str(key)]["x_dkimalignfail"]) + "," +  str(report_map[str(key)]["x_dkimpolicypass"]));
  
  return

def write_dmarc_map(dmarc_map,outdir,reportdate):

  filename = outdir + "/report-details-" + reportdate + ".csv"
  file1 = open(filename, 'w')
  file1.write("Organization,Date.Begin,Date.End,Domain,Policy,SourceIP,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Auth.Pass,SPF.Auth.Fail,SPF.Align.Pass,SPF.Align.Fail,SPF.Policy.Pass,DKIM.Auth.Pass,DKIM.Auth.Fail,DKIM.Align.Pass,DKIM.Align.Fail,DKIM.Policy.Pass\n");
  
  for org in dmarc_map:
    for domain in dmarc_map[org]:
        header = org + "," + dmarc_map[org][domain][0] + "," + dmarc_map[org][domain][1] + "," + domain + "," + \
        dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
          if report_map[str(key)]["count"] > 0:
            x_dmarcrate = str(int((report_map[str(key)]["x_dmarcpass"]/report_map[str(key)]["count"])*100))
          else:
            x_dmarcrate = "0"            
          file1.write(header + "," + str(key) + "," + str(report_map[str(key)]["count"]) + "," + str(report_map[str(key)]["x_dmarcpass"]) + "," \
           + str(report_map[str(key)]["x_dmarcfail"]) + "," + x_dmarcrate + "%" + "," \
           + str(report_map[str(key)]["x_spfauthpass"]) + "," + str(report_map[str(key)]["x_spfauthfail"]) + "," \
           + str(report_map[str(key)]["x_spfalignpass"]) + "," + str(report_map[str(key)]["x_spfalignfail"]) + "," \
           + str(report_map[str(key)]["x_spfpolicypass"]) + "," + str(report_map[str(key)]["x_dkimauthpass"]) + "," \
           + str(report_map[str(key)]["x_dkimauthfail"]) + "," + str(report_map[str(key)]["x_dkimalignpass"]) + "," \
           + str(report_map[str(key)]["x_dkimalignfail"]) + "," +  str(report_map[str(key)]["x_dkimpolicypass"]) + "\n");
  file1.close()
  return filename

def write_dmarc_map_domain(dmarc_map, domain,outdir,reportdate):

  filename = outdir + "/report-details-" + domain + "-" + reportdate + ".csv"
  file1 = open(filename, 'w')
  file1.write("Organization,Date.Begin,Date.End,Domain,Policy,SourceIP,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Auth.Pass,SPF.Auth.Fail,SPF.Align.Pass,SPF.Align.Fail,SPF.Policy.Pass,DKIM.Auth.Pass,DKIM.Auth.Fail,DKIM.Align.Pass,DKIM.Align.Fail,DKIM.Policy.Pass\n");
  
  for org in dmarc_map:
    if domain in dmarc_map[org]:
        header = org + "," + dmarc_map[org][domain][0] + "," + dmarc_map[org][domain][1] + "," + domain + "," + \
        dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
          if report_map[str(key)]["count"] > 0:
            x_dmarcrate = str(int((report_map[str(key)]["x_dmarcpass"]/report_map[str(key)]["count"])*100))
          else:
            x_dmarcrate = "0"  
          file1.write(header + "," + str(key) + "," + str(report_map[str(key)]["count"]) + "," + str(report_map[str(key)]["x_dmarcpass"]) + "," \
           + str(report_map[str(key)]["x_dmarcfail"]) + "," + x_dmarcrate + "%" + "," \
           + str(report_map[str(key)]["x_spfauthpass"]) + "," + str(report_map[str(key)]["x_spfauthfail"]) + "," \
           + str(report_map[str(key)]["x_spfalignpass"]) + "," + str(report_map[str(key)]["x_spfalignfail"]) + "," \
           + str(report_map[str(key)]["x_spfpolicypass"]) + "," + str(report_map[str(key)]["x_dkimauthpass"]) + "," \
           + str(report_map[str(key)]["x_dkimauthfail"]) + "," + str(report_map[str(key)]["x_dkimalignpass"]) + "," \
           + str(report_map[str(key)]["x_dkimalignfail"]) + "," +  str(report_map[str(key)]["x_dkimpolicypass"]) + "\n");
  
  file1.close()
  return filename

def print_dmarc_map_sourceip(dmarc_map, key):
  print("Organization,Date.Begin,Date.End,Domain,Policy,SourceIP,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Auth.Pass,SPF.Auth.Fail,SPF.Align.Pass,SPF.Align.Fail,SPF.Policy.Pass,DKIM.Auth.Pass,DKIM.Auth.Fail,DKIM.Align.Pass,DKIM.Align.Fail,DKIM.Policy.Pass");
  
  for org in dmarc_map:
    for domain in dmarc_map[org]:
        header = org + "," + dmarc_map[org][domain][0] + "," + dmarc_map[org][domain][1] + "," + domain + "," + \
        dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        if key in dmarc_map[org][domain][3].keys():
          print(header + "," + str(key) + "," + str(report_map[str(key)]["count"]) + "," + str(report_map[str(key)]["x_dmarcpass"]) + "," \
           + str(report_map[str(key)]["x_dmarcfail"]) + "," + str(int((report_map[str(key)]["x_dmarcpass"]/report_map[str(key)]["count"])*100)) + "%" + "," \
           + str(report_map[str(key)]["x_spfauthpass"]) + "," + str(report_map[str(key)]["x_spfauthfail"]) + "," \
           + str(report_map[str(key)]["x_spfalignpass"]) + "," + str(report_map[str(key)]["x_spfalignfail"]) + "," \
           + str(report_map[str(key)]["x_spfpolicypass"]) + "," + str(report_map[str(key)]["x_dkimauthpass"]) + "," \
           + str(report_map[str(key)]["x_dkimauthfail"]) + "," + str(report_map[str(key)]["x_dkimalignpass"]) + "," \
           + str(report_map[str(key)]["x_dkimalignfail"]) + "," +  str(report_map[str(key)]["x_dkimpolicypass"]));
  
  return

def print_dmarc_map_globalmetrics_domain(dmarc_map,domain):
  print("Domain,Policy,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Align.Pass,SPF.Align.Fail,DKIM.Align.Pass,DKIM.Align.Fail");

  policy = ""
  x_count = 0
  x_dmarcpass = 0
  x_dmarcfail = 0
  x_dmarcrate = 0
  x_spfalignpass = 0
  x_spfalignfail = 0
  x_dkimalignpass = 0
  x_dkimalignfail = 0
  
  for org in dmarc_map:
    if domain in dmarc_map[org]:
        policy = dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
          x_count       = x_count       + report_map[str(key)]["count"]
          x_dmarcpass   = x_dmarcpass   + report_map[str(key)]["x_dmarcpass"]
          x_dmarcfail   = x_dmarcfail   + report_map[str(key)]["x_dmarcfail"]
          x_spfalignpass = x_spfalignpass + report_map[str(key)]["x_spfalignpass"]
          x_spfalignfail = x_spfalignfail + report_map[str(key)]["x_spfalignfail"]
          x_dkimalignpass = x_dkimalignpass + report_map[str(key)]["x_dkimalignpass"]
          x_dkimalignfail = x_dkimalignfail + report_map[str(key)]["x_dkimalignfail"]

  x_dmarcrate = int(x_dmarcpass / x_count * 100)    
  print(domain + "," + policy + "," + str(x_count) + "," + str(x_dmarcpass) + "," + str(x_dmarcfail) + "," + str(x_dmarcrate) + "%" + "," \
           + str(x_spfalignpass) + "," + str(x_spfalignfail) + "," + str(x_dkimalignpass) + "," + str(x_dkimalignfail) );
  
  return

def write_dmarc_map_globalmetrics_domain(dmarc_map,domain,outdir,reportdate):

  filename = outdir + "/report-global-" + domain + "-" + reportdate + ".csv"
  file1 = open(filename, 'w')
  file1.write("ReportDate,Domain,Policy,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Align.Pass,SPF.Align.Fail,DKIM.Align.Pass,DKIM.Align.Fail\n");

  policy = ""
  x_count = 0
  x_dmarcpass = 0
  x_dmarcfail = 0
  x_dmarcrate = 0
  x_spfalignpass = 0
  x_spfalignfail = 0
  x_dkimalignpass = 0
  x_dkimalignfail = 0
  
  for org in dmarc_map:
    if domain in dmarc_map[org]:
        policy = dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
          x_count       = x_count       + report_map[str(key)]["count"]
          x_dmarcpass   = x_dmarcpass   + report_map[str(key)]["x_dmarcpass"]
          x_dmarcfail   = x_dmarcfail   + report_map[str(key)]["x_dmarcfail"]
          x_spfalignpass = x_spfalignpass + report_map[str(key)]["x_spfalignpass"]
          x_spfalignfail = x_spfalignfail + report_map[str(key)]["x_spfalignfail"]
          x_dkimalignpass = x_dkimalignpass + report_map[str(key)]["x_dkimalignpass"]
          x_dkimalignfail = x_dkimalignfail + report_map[str(key)]["x_dkimalignfail"]

  x_dmarcrate = int(x_dmarcpass / x_count * 100)    
  file1.write(reportdate + "," + domain + "," + policy + "," + str(x_count) + "," + str(x_dmarcpass) + "," + str(x_dmarcfail) + "," + str(x_dmarcrate) + "%" + "," \
           + str(x_spfalignpass) + "," + str(x_spfalignfail) + "," + str(x_dkimalignpass) + "," + str(x_dkimalignfail) + "\n");
  
  file1.close()
  return filename

def print_dmarc_map_topsender(dmarc_map, domain):

  top_map = defaultdict()
  
  for org in dmarc_map:
    if domain in dmarc_map[org]:
        header = domain + "," + dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
            if str(key) in top_map:
                top_map[str(key)]["count"] = top_map[str(key)]["count"] + report_map[str(key)]["count"];
                top_map[str(key)]["x_dmarcpass"] = top_map[str(key)]["x_dmarcpass"] + report_map[str(key)]["x_dmarcpass"];
                top_map[str(key)]["x_dmarcfail"] = top_map[str(key)]["x_dmarcfail"] + report_map[str(key)]["x_dmarcfail"];
                top_map[str(key)]["x_spfauthpass"] = top_map[str(key)]["x_spfauthpass"] + report_map[str(key)]["x_spfauthpass"];
                top_map[str(key)]["x_spfauthfail"] = top_map[str(key)]["x_spfauthfail"] + report_map[str(key)]["x_spfauthfail"];
                top_map[str(key)]["x_spfalignpass"] = top_map[str(key)]["x_spfalignpass"] + report_map[str(key)]["x_spfalignpass"];
                top_map[str(key)]["x_spfalignfail"] = top_map[str(key)]["x_spfalignfail"] + report_map[str(key)]["x_spfalignfail"];
                top_map[str(key)]["x_spfpolicypass"] = top_map[str(key)]["x_spfpolicypass"] + report_map[str(key)]["x_spfpolicypass"];
                top_map[str(key)]["x_dkimauthpass"] = top_map[str(key)]["x_dkimauthpass"] + report_map[str(key)]["x_dkimauthpass"];
                top_map[str(key)]["x_dkimauthfail"] = top_map[str(key)]["x_dkimauthfail"] + report_map[str(key)]["x_dkimauthfail"];
                top_map[str(key)]["x_dkimalignpass"] = top_map[str(key)]["x_dkimalignpass"] + report_map[str(key)]["x_dkimalignpass"];
                top_map[str(key)]["x_dkimalignfail"] = top_map[str(key)]["x_dkimalignfail"] + report_map[str(key)]["x_dkimalignfail"];
                top_map[str(key)]["x_dkimpolicypass"] = top_map[str(key)]["x_dkimpolicypass"] + report_map[str(key)]["x_dkimpolicypass"];
          
            else:              
                top_map[str(key)] = {}
                top_map[str(key)]["count"] = report_map[str(key)]["count"]
                top_map[str(key)]["x_dmarcpass"] = report_map[str(key)]["x_dmarcpass"]
                top_map[str(key)]["x_dmarcfail"] = report_map[str(key)]["x_dmarcfail"]
                top_map[str(key)]["x_spfauthpass"] = report_map[str(key)]["x_spfauthpass"]
                top_map[str(key)]["x_spfauthfail"] = report_map[str(key)]["x_spfauthfail"]
                top_map[str(key)]["x_spfalignpass"] = report_map[str(key)]["x_spfalignpass"]
                top_map[str(key)]["x_spfalignfail"] = report_map[str(key)]["x_spfalignfail"]
                top_map[str(key)]["x_spfpolicypass"] = report_map[str(key)]["x_spfpolicypass"]
                top_map[str(key)]["x_dkimauthpass"] = report_map[str(key)]["x_dkimauthpass"]
                top_map[str(key)]["x_dkimauthfail"] = report_map[str(key)]["x_dkimauthfail"]
                top_map[str(key)]["x_dkimalignpass"] = report_map[str(key)]["x_dkimalignpass"]
                top_map[str(key)]["x_dkimalignfail"] = report_map[str(key)]["x_dkimalignfail"]
                top_map[str(key)]["x_dkimpolicypass"] = report_map[str(key)]["x_dkimpolicypass"]


  print("Domain,Policy,SourceIP,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Auth.Pass,SPF.Auth.Fail,SPF.Align.Pass,SPF.Align.Fail,SPF.Policy.Pass,DKIM.Auth.Pass,DKIM.Auth.Fail,DKIM.Align.Pass,DKIM.Align.Fail,DKIM.Policy.Pass");

  temp_map = dict(reversed(sorted(top_map.items(), key=lambda item: item[1]["count"])))
  i=0
  for key in temp_map.keys():
      if i<10:
          print(header + "," + key + "," + str(temp_map[key]["count"]) + "," + str(temp_map[str(key)]["x_dmarcpass"]) + "," \
           + str(temp_map[str(key)]["x_dmarcfail"]) + "," + str(int((temp_map[str(key)]["x_dmarcpass"]/temp_map[str(key)]["count"])*100)) + "%" + "," \
           + str(temp_map[str(key)]["x_spfauthpass"]) + "," + str(temp_map[str(key)]["x_spfauthfail"]) + "," \
           + str(temp_map[str(key)]["x_spfalignpass"]) + "," + str(temp_map[str(key)]["x_spfalignfail"]) + "," \
           + str(temp_map[str(key)]["x_spfpolicypass"]) + "," + str(temp_map[str(key)]["x_dkimauthpass"]) + "," \
           + str(temp_map[str(key)]["x_dkimauthfail"]) + "," + str(temp_map[str(key)]["x_dkimalignpass"]) + "," \
           + str(temp_map[str(key)]["x_dkimalignfail"]) + "," +  str(temp_map[str(key)]["x_dkimpolicypass"]));
          i=i+1
  return

def write_dmarc_map_topsender(dmarc_map, domain,outdir,reportdate):

  top_map = defaultdict()
  
  for org in dmarc_map:
    if domain in dmarc_map[org]:
        header = reportdate + "," + domain + "," + dmarc_map[org][domain][2]
        report_map = dmarc_map[org][domain][3]
        for key in dmarc_map[org][domain][3].keys():
            if str(key) in top_map:
                top_map[str(key)]["count"] = top_map[str(key)]["count"] + report_map[str(key)]["count"];
                top_map[str(key)]["x_dmarcpass"] = top_map[str(key)]["x_dmarcpass"] + report_map[str(key)]["x_dmarcpass"];
                top_map[str(key)]["x_dmarcfail"] = top_map[str(key)]["x_dmarcfail"] + report_map[str(key)]["x_dmarcfail"];
                top_map[str(key)]["x_spfauthpass"] = top_map[str(key)]["x_spfauthpass"] + report_map[str(key)]["x_spfauthpass"];
                top_map[str(key)]["x_spfauthfail"] = top_map[str(key)]["x_spfauthfail"] + report_map[str(key)]["x_spfauthfail"];
                top_map[str(key)]["x_spfalignpass"] = top_map[str(key)]["x_spfalignpass"] + report_map[str(key)]["x_spfalignpass"];
                top_map[str(key)]["x_spfalignfail"] = top_map[str(key)]["x_spfalignfail"] + report_map[str(key)]["x_spfalignfail"];
                top_map[str(key)]["x_spfpolicypass"] = top_map[str(key)]["x_spfpolicypass"] + report_map[str(key)]["x_spfpolicypass"];
                top_map[str(key)]["x_dkimauthpass"] = top_map[str(key)]["x_dkimauthpass"] + report_map[str(key)]["x_dkimauthpass"];
                top_map[str(key)]["x_dkimauthfail"] = top_map[str(key)]["x_dkimauthfail"] + report_map[str(key)]["x_dkimauthfail"];
                top_map[str(key)]["x_dkimalignpass"] = top_map[str(key)]["x_dkimalignpass"] + report_map[str(key)]["x_dkimalignpass"];
                top_map[str(key)]["x_dkimalignfail"] = top_map[str(key)]["x_dkimalignfail"] + report_map[str(key)]["x_dkimalignfail"];
                top_map[str(key)]["x_dkimpolicypass"] = top_map[str(key)]["x_dkimpolicypass"] + report_map[str(key)]["x_dkimpolicypass"];
          
            else:              
                top_map[str(key)] = {}
                top_map[str(key)]["count"] = report_map[str(key)]["count"]
                top_map[str(key)]["x_dmarcpass"] = report_map[str(key)]["x_dmarcpass"]
                top_map[str(key)]["x_dmarcfail"] = report_map[str(key)]["x_dmarcfail"]
                top_map[str(key)]["x_spfauthpass"] = report_map[str(key)]["x_spfauthpass"]
                top_map[str(key)]["x_spfauthfail"] = report_map[str(key)]["x_spfauthfail"]
                top_map[str(key)]["x_spfalignpass"] = report_map[str(key)]["x_spfalignpass"]
                top_map[str(key)]["x_spfalignfail"] = report_map[str(key)]["x_spfalignfail"]
                top_map[str(key)]["x_spfpolicypass"] = report_map[str(key)]["x_spfpolicypass"]
                top_map[str(key)]["x_dkimauthpass"] = report_map[str(key)]["x_dkimauthpass"]
                top_map[str(key)]["x_dkimauthfail"] = report_map[str(key)]["x_dkimauthfail"]
                top_map[str(key)]["x_dkimalignpass"] = report_map[str(key)]["x_dkimalignpass"]
                top_map[str(key)]["x_dkimalignfail"] = report_map[str(key)]["x_dkimalignfail"]
                top_map[str(key)]["x_dkimpolicypass"] = report_map[str(key)]["x_dkimpolicypass"]

  filename = outdir + "/report-topsender-" + domain + "-" + reportdate + ".csv"
  file1 = open(filename, 'w')
  file1.write("ReportDate,Domain,Policy,SourceIP,Email Volume,DMARC.Pass,DMARC.Fail,DMARC.Rate,SPF.Auth.Pass,SPF.Auth.Fail,SPF.Align.Pass,SPF.Align.Fail,SPF.Policy.Pass,DKIM.Auth.Pass,DKIM.Auth.Fail,DKIM.Align.Pass,DKIM.Align.Fail,DKIM.Policy.Pass\n");

  temp_map = dict(reversed(sorted(top_map.items(), key=lambda item: item[1]["count"])))
  i=0
  for key in temp_map.keys():
      if i<10:
          file1.write(header + "," + key + "," + str(temp_map[key]["count"]) + "," + str(temp_map[str(key)]["x_dmarcpass"]) + "," \
           + str(temp_map[str(key)]["x_dmarcfail"]) + "," + str(int((temp_map[str(key)]["x_dmarcpass"]/temp_map[str(key)]["count"])*100)) + "%" + "," \
           + str(temp_map[str(key)]["x_spfauthpass"]) + "," + str(temp_map[str(key)]["x_spfauthfail"]) + "," \
           + str(temp_map[str(key)]["x_spfalignpass"]) + "," + str(temp_map[str(key)]["x_spfalignfail"]) + "," \
           + str(temp_map[str(key)]["x_spfpolicypass"]) + "," + str(temp_map[str(key)]["x_dkimauthpass"]) + "," \
           + str(temp_map[str(key)]["x_dkimauthfail"]) + "," + str(temp_map[str(key)]["x_dkimalignpass"]) + "," \
           + str(temp_map[str(key)]["x_dkimalignfail"]) + "," +  str(temp_map[str(key)]["x_dkimpolicypass"]) + "\n");
          i=i+1
  file1.close()
  return filename

# Reads .xml files and creates a structure with all information
def readdmarcreports(tempdir):

  dmarc_map = defaultdict(dict)
  
  files=os.listdir(tempdir)
  for file in files:
    if file.endswith('.xml'):
            filePath=tempdir+'/'+file

            fd = open(filePath,'rb')
            source = io.TextIOWrapper(fd, newline="")
            # get an iterable and turn it into an iterator
            meta_fields = get_meta(iter(etree.iterparse(source, events=("start", "end"))));
            if not meta_fields:
              print >> sys.stderr, "Error: No valid 'policy_published' and 'report_metadata' xml tags found; File: " + args.dmarcfile 
              sys.exit(1)
            fd.close()
            org, begin, end, domain, policy = meta_fields.split(",")

            fd = open(filePath,'rb')
            source = io.TextIOWrapper(fd, newline="")   
            report_map = print_record(iter(etree.iterparse(source, events=("start", "end"))), meta_fields, args)
            fd.close()
            
            if ( org in dmarc_map ) & ( domain in dmarc_map[org] ):
                old_report_map = dmarc_map[org][domain][3]
                for source_ip in report_map.keys():
                    if str(source_ip) in old_report_map:
                        old_report_map[str(source_ip)]["count"] = report_map[str(source_ip)]["count"] + old_report_map[str(source_ip)]["count"];
                        old_report_map[str(source_ip)]["x_dmarcpass"] = report_map[str(source_ip)]["x_dmarcpass"] + old_report_map[str(source_ip)]["x_dmarcpass"];
                        old_report_map[str(source_ip)]["x_dmarcfail"] = report_map[str(source_ip)]["x_dmarcfail"] + old_report_map[str(source_ip)]["x_dmarcfail"];
                        old_report_map[str(source_ip)]["x_spfauthpass"] = report_map[str(source_ip)]["x_spfauthpass"] + old_report_map[str(source_ip)]["x_spfauthpass"];
                        old_report_map[str(source_ip)]["x_spfauthfail"] = report_map[str(source_ip)]["x_spfauthfail"] + old_report_map[str(source_ip)]["x_spfauthfail"];
                        old_report_map[str(source_ip)]["x_spfalignpass"] = report_map[str(source_ip)]["x_spfalignpass"] + old_report_map[str(source_ip)]["x_spfalignpass"];
                        old_report_map[str(source_ip)]["x_spfalignfail"] = report_map[str(source_ip)]["x_spfalignfail"] + old_report_map[str(source_ip)]["x_spfalignfail"];
                        old_report_map[str(source_ip)]["x_spfpolicypass"] = report_map[str(source_ip)]["x_spfpolicypass"] + old_report_map[str(source_ip)]["x_spfpolicypass"];
                        old_report_map[str(source_ip)]["x_dkimauthpass"] = report_map[str(source_ip)]["x_dkimauthpass"] + old_report_map[str(source_ip)]["x_dkimauthpass"];
                        old_report_map[str(source_ip)]["x_dkimauthfail"] = report_map[str(source_ip)]["x_dkimauthfail"] + old_report_map[str(source_ip)]["x_dkimauthfail"];
                        old_report_map[str(source_ip)]["x_dkimalignpass"] = report_map[str(source_ip)]["x_dkimalignpass"] + old_report_map[str(source_ip)]["x_dkimalignpass"];
                        old_report_map[str(source_ip)]["x_dkimalignfail"] = report_map[str(source_ip)]["x_dkimalignfail"] + old_report_map[str(source_ip)]["x_dkimalignfail"];
                        old_report_map[str(source_ip)]["x_dkimpolicypass"] = report_map[str(source_ip)]["x_dkimpolicypass"] + old_report_map[str(source_ip)]["x_dkimpolicypass"];
                        dmarc_map[org][domain][3]=old_report_map
                    else:
                        old_report_map[str(source_ip)]=report_map[str(source_ip)]
                        dmarc_map[org][domain][3]=old_report_map
            else:
                dmarc_map[org][domain] = [ begin, end, policy , report_map] 

  return dmarc_map

# Send email with global metrics, top senders and details in attach
def senddmarcreport(outdir,domain,reportdate):
  global ews_url
  global userad
  global password
  global primary_smtp_address
  global destinationaddress

  BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter
  ews_auth_type = 'NTLM'

  cred = Credentials(userad, password)
  config = Configuration(service_endpoint=ews_url, credentials=cred, auth_type=ews_auth_type)
  acc = Account(
        primary_smtp_address=primary_smtp_address, 
        config=config, autodiscover=False, 
        access_type=DELEGATE,
  )

  a = pd.read_csv(outdir + "/" + "report-topsender-" + domain + "-" + reportdate + ".csv", error_bad_lines=False) 
  b = pd.read_csv(outdir + "/" + "report-global-" + domain + "-" + reportdate + ".csv", error_bad_lines=False)

  html_file = "<html><header>DMARC Domain Report</header><body><br><b>Global metrics</b><br>" \
        + b.to_html(index=False)  \
	+ "<br><b>Top Senders</b><br>" + a.to_html(index=False) + "</body></html>"

  #print(html_file)

  to_recipients =[]
  to_recipients.append(Mailbox(email_address=destinationaddress))
  to_recipients.append(Mailbox(email_address=primary_smtp_address))
  m1 = Message(
     account=acc,
     subject='Daily DMARC Domain Report',
     body=HTMLBody(html_file),
     to_recipients=to_recipients   
  )
  with open(outdir + "/" + "report-details-" + domain + "-" + reportdate + ".csv", 'rb') as f:
               attachcontent = f.read()
  m1.attach(FileAttachment(name="report-details-" + domain + "-" + reportdate + ".csv", content=attachcontent))

  m1.send()

  return

def main():
  global args
  global user
  global userad
  global password
  global reportdate
  global report_domain
  global primary_smtp_address
  global destinationaddress
  global searchcriteria
  
  options = argparse.ArgumentParser(epilog='Example: \
  %(prog)s  -m dmarc@example.com -u DOMAIN\\user -p password -s \"19-Sep-2021\" -d example.com -f from@example.com -t to@example.com')
  options.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")
  options.add_argument("--quiet", help="supress all comments (stdout)", action="store_true")
  options.add_argument("-m", "--mailbox", help="mailbox with dmarc reports",type=str)
  options.add_argument("-u", "--user", help="user allowed to read mailbox",type=str)
  options.add_argument("-p", "--password", help="user password")
  options.add_argument("-s", "--search", help="default previous day, format DD-MMM-YYYY")
  options.add_argument("-d", "--domain", help="report associated to domain")
  options.add_argument("-f", "--fromaddr", help="mail from")
  options.add_argument("-t", "--toaddr", help="mail to")
  options.add_argument("-P", "--pwdfile", help="A file that stores user password. If not set, the user is prompted to provide a passwd")
  args = options.parse_args()
 
  if args.mailbox:
    user = args.mailbox
  if args.user:
    userad = args.user
  if args.password:
    password = args.password
  if args.search:
    reportdate = args.search
    searchcriteria = "ON " + reportdate + " SUBJECT \"Report Domain\""
  if args.domain:
    report_domain = args.domain
  if args.fromaddr:
    primary_smtp_address = args.fromaddr
  if args.toaddr:
    destinationaddress = args.toaddr
 
  #print(" user:" + user)
  #print(" userad:" + userad)
  #print(" password:" + password)
  #print(" reportdate:" + reportdate)
  #print(" report_domain:" + report_domain)
  #print(" primary_smtp_address:" + primary_smtp_address)
  #print(" destinationaddress:" + destinationaddress)
  
  # redirect stdout to /dev/null
  #if args.quiet:
  #  f = open(os.devnull, 'w')
  #  sys.stdout = f

  # Get XML files with DMARC Aggregated reports sent by external entities
  getdmarcreports()
  # Create a internal structure with the data collected
  dmarc_map = readdmarcreports(tempdir)
  # Generate csv files the data collected
  report1 = write_dmarc_map(dmarc_map,tempdir,reportdate)
  report2 = write_dmarc_map_domain(dmarc_map, report_domain,tempdir,reportdate)
  report3 = write_dmarc_map_topsender(dmarc_map, report_domain,tempdir,reportdate)
  report4 = write_dmarc_map_globalmetrics_domain(dmarc_map,report_domain,tempdir,reportdate)
  # Send email with different metrics for a domain
  senddmarcreport(tempdir,report_domain,reportdate)

  # Clean up 
  files=os.listdir(tempdir)
  for file in files:
    if file.endswith('.xml'):
      os.remove(file)
  os.remove(report1)
  os.remove(report2)
  os.remove(report3)
  os.remove(report4)
  

# entry point
if __name__ == "__main__":
  main()
