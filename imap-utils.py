#!/usr/bin/python3
# This work is licensed under GPL 3, see LICENSE
# Author: Michael Walz <code@serpedon.de>, © 2016

__all__ = ['scan_imap']

import imaplib
import email
import re
list_response_pattern = re.compile(r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)')
def parse_list_response(line):
    flags, delimiter, mailbox_name = list_response_pattern.match(line).groups()
    mailbox_name = mailbox_name.strip('"')
    return (flags, delimiter, mailbox_name)

def scan_imap(imap4, imap_search, store_command = None)
    """
        imap4 an IMAP4-instance with performed login.
            e.g. imap4 = imaplib.IMAP4_SSL("imap.example.com", 993)
                 imap4.login("username","password")
                 (after usage, imap4.close(); imap4.logout() is closed the connection)

        imap_search a search query for messages which shall be investigated
            e.g. imap_search = "(Flagged Undeleted)" # search flagged emails and return uids
            or   imap_search = "(Flagged Undeleted)" # search flagged emails and return uids

        Returns a list of dictionaries of all messages which match imap_search
            contained keys: Folder, Id, From, To, Subject, Date
    """

    foundMsg = []

    result, mailbox_list = m.list()
    if not result == 'OK' : raise RuntimeError('m.list(): ' + result) 

    for mailbox in mailbox_list:
        (flags, delimiter, mailbox_name) = parse_list_response(mailbox.decode('utf-8'))

        result, data = m.select('"' + mailbox_name + '"', readonly=True)
        if not result == 'OK' : raise RuntimeError('m.select(' + mailbox_name + '): ' + result) 

        result, data = m.uid('search', None, search_imap)
        if not result == 'OK' : raise RuntimeError("m.uid(search, ...) in " + mailbox_name + '): ' + result)

        for num in data[0].split():
        result, data = m.uid('fetch', num, '(BODY[HEADER])') # '(RFC822)' would load the whole message
        if not result == 'OK' : raise RuntimeError("m.uid(fetch, ...) in " + mailbox_name + '): ' + result) 

        email_message = email.message_from_bytes(data[0][1])
        foundMsg.append( dict(Folder=mailbox_name, Id=email_message['Message-ID'], From=email_message['From'], To=email_message['To'], Subject=email_message['Subject'], Date=email_message['Date']) )

    return foundMsg
