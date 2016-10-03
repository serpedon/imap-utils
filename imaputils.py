#!/usr/bin/python3
# This work is licensed under GPL 3, see LICENSE
# Author: Michael Walz <code@serpedon.de>, © 2016

__all__ = ['scan_imap', 'store_imap_to_mbox', 'backup_imap']

import imaplib
import email
import re
list_response_pattern = re.compile(r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)')


def parse_list_response(line):
    flags, delimiter, mailbox_name = list_response_pattern.match(line).groups()
    mailbox_name = mailbox_name.strip('"')
    return (flags, delimiter, mailbox_name)

def clean(string) :
    if type(string) == type(str()) :
        return string.strip()
    return string

def scan_imap(imap4, imap_search, store_command = None, return_found_msg = True, return_only_headers = True, mailbox_name = None) : 
    """
        The method scannes an imap-mailbox for messages.

        Parameters: 
        imap4 is an IMAP4-instance with performed login.
            e.g. imap4 = imaplib.IMAP4_SSL("imap.example.com", 993)
                 imap4.login("username","password")
                 (after usage, imap4.close(); imap4.logout() is closed the connection)

        imap_search a search query for messages which shall be investigated
            e.g. imap_search = "(Flagged Undeleted)" # search flagged emails and return uids
            or   imap_search = "(Header Message-ID <messageID@example.com>)" # search for a specific Message-ID

        store_command if given, a store command is issued on all found messages
            e.g. store_command = ('+FLAGS', '\\Flagged') # to flag all found messages

        If return_found_msg == True, the function returns a list of all messages which match imap_search
            each message is represented by a dictionary with keys: 'Folder', 'Id', 'From', 'To', 'Subject', 'Date'
    
        If return_only_headers == True, the returned message contains only the headers (default).
        If return_only_headers == False, the full messages is also returned via the key 'Message'
        (return_only_headers is only evaluated when return_found_msg == True is set)

        mailbox_name is the name of an imap folder which can be given to imap4.select(mailbox_name).
        If set to None (default), the scan is performed for all imap folders.
    """

    foundMsg = []

    if mailbox_name is not None :
        mailbox_list = [ b'() "." ' + mailbox_name.encode('utf-8') ]
    else :
        result, mailbox_list = imap4.list()
        if not result == 'OK' : raise RuntimeError('imap4.list(): ' + result) 
    
    for mailbox in mailbox_list:
        (flags, delimiter, mailbox_name) = parse_list_response(mailbox.decode('utf-8'))

        result, data = imap4.select('"' + mailbox_name + '"', readonly=(store_command is None))
        if not result == 'OK' : raise RuntimeError('imap4.select(' + mailbox_name + '): ' + result) 

        result, data = imap4.uid('search', None, imap_search)
        if not result == 'OK' : raise RuntimeError("imap4.uid(search, ...) in " + mailbox_name + '): ' + result)

        for num in data[0].split():
            if store_command is not None :
                result, data = imap4.uid('store', num, store_command[0], store_command[1])
                if not result == 'OK' : raise RuntimeError('imap4.uid(store, ' + str(num) + ', ' + store_command + '): ' + result) 

            if return_found_msg :
                result, data = imap4.uid('fetch', num, '(BODY[HEADER])' if return_only_headers else '(RFC822)') # '(BODY[HEADER])' reads only the headers; '(RFC822)' loads the whole message
                if not result == 'OK' : raise RuntimeError("imap4.uid(fetch, ...) in " + mailbox_name + '): ' + result) 

                email_message = email.message_from_bytes(data[0][1])
                Message = None if return_only_headers else email_message
                foundMsg.append( dict(Folder=mailbox_name, Id=clean(email_message['Message-ID']), From=clean(email_message['From']), To=clean(email_message['To']), Subject=clean(email_message['Subject']), Date=clean(email_message['Date']), Message=Message) )

    if return_found_msg :
        return foundMsg

def backup_imap(imap4, backup_folder, deleted_folder = '_deleted') :
    """
        The method backups an imap-mailbox to local disk.

        Parameters: 
        imap4 is an IMAP4-instance with performed login.
            e.g. imap4 = imaplib.IMAP4_SSL("imap.example.com", 993)
                 imap4.login("username","password")
                 (after usage, imap4.close(); imap4.logout() is closed the connection)

        backup_folder is a folder on the local disk where the imap-mailbox shall be backuped to.
        Incremental backups are possible.
        
        deleted_folder gives the name of the subfolder were messages that already exist on the local 
        disk but were deleted on the imap-server are moved to.
    """

    # imports
    import os

    # helper functions
    def gen_filename(mail_dict) :
        """ Generates a filename from a dictionary with keys 'Id', 'Date', 'Folder' """
        from email.utils import parsedate_to_datetime
        dateObj = parsedate_to_datetime(mail_dict['Date'])
        fulldate = dateObj.strftime("%Y-%m-%d_%H.%M_utc%z")
        year = dateObj.strftime("%Y")

        Id = mail_dict['Id'].replace('<', '').replace('>', '').replace('%', '').replace('/', '-').replace(' ', '')

        Folder = mail_dict['Folder'].replace(' ', '_')

        return os.path.join(Folder, year, fulldate + "_" + Id + '.eml')

    def create_dir_if_not_exist(folder) :
        """ Creates a directory and all its parents if necessary. """
        if not os.path.exists(folder) :
            os.makedirs(folder)

    def  fix_cte(message) :
        """
            Cycles through all internal messages, tests if they are convertable to string,
            and if not (because of a KeyError) adds the field 'content-transfer-encoding'.
        """
        if message.is_multipart() :
            for payload in message.get_payload() :
                fix_cte(payload)
        try :
            string = str(message)
        except KeyError as err :
            if len(err.args) > 0 and err.args[0] == 'content-transfer-encoding' : 
                if 'content-transfer-encoding' not in message : 
                    print('Fixing payload: adding key content-transfer-encoding')
                    message['content-transfer-encoding'] = None

                   
    # Search for all messages that are not deleted
    allMsgHeaders = scan_imap(imap4, imap_search="(Undeleted)")

    msg_already_existing = 0
    msg_downloaded = 0

    paths_of_all_msg = set()

    # Download new messages (if not available locally)
    for msg in allMsgHeaders:
        from email import generator
        filename = gen_filename(msg)
        full_path = os.path.join(backup_folder, filename)
        directory = os.path.dirname(full_path)
        if full_path in paths_of_all_msg :
            print('Warning: %s has been found multiple times on the server' % filename)
            continue	
        paths_of_all_msg |= { full_path }
        create_dir_if_not_exist(directory)
        if os.path.exists(full_path) :
            msg_already_existing += 1
        else :
            msg_downloaded += 1
            print('%s is new on server --> downloading' % (filename))
            full_msgs = scan_imap(imap4, imap_search="(Header Message-ID \"" + msg['Id'] + "\")", return_only_headers = False, mailbox_name = msg['Folder'])
            for full_msg in full_msgs :
                filename2 = gen_filename(full_msg)
                if not (filename == filename2) : raise RuntimeError('Error: filenames do not match: ' + filename + ' ; ' + filename2) 

                full_path_tilde = full_path + '~'
                with open(full_path_tilde, 'w') as outfile :
                    message = full_msg['Message']
                    gen = generator.Generator(outfile, mangle_from_=False)
                    try :
                        gen.flatten(message)
                    except KeyError:
                        fix_cte(message)
                        gen.flatten(message)
                       
                os.rename(full_path_tilde, full_path)
                break


    # Update local list of paths of all messages with all parent directories
    for path in list(paths_of_all_msg) :
        dirname = path
        old_dirname = None
        while dirname != old_dirname :
            old_dirname = dirname
            dirname = os.path.dirname(dirname)
            paths_of_all_msg |= { dirname }

    # Move all file in 'deleted'-subdirectories which do not exist on the imap-server anymore
    msg_keeping = 0
    msg_move_to_deleted = 0
    def move_deleted_messages(folder) :
        nonlocal msg_keeping
        nonlocal msg_move_to_deleted
        for filename in os.listdir(folder) :
            if filename != deleted_folder :
                full_path = os.path.join(folder, filename)
                if full_path in paths_of_all_msg :
                    if not os.path.isdir(full_path) : msg_keeping += 1
                else :
                    msg_move_to_deleted += 1
                    parent_dir = os.path.dirname(full_path)
                    deleted_dir = os.path.join(parent_dir, deleted_folder)
                    create_dir_if_not_exist(deleted_dir)
                    print('%s was removed from server --> moving to %s' % (full_path[(len(backup_folder)+1):], deleted_folder))
                    os.rename(full_path, os.path.join(deleted_dir, filename))
                
                if os.path.isdir(full_path) :
                    move_deleted_messages(full_path)

    move_deleted_messages(backup_folder)

    print("Summary (downloads):")
    print("  A=%d messages have been downloaded in the current run" % msg_downloaded)
    print("  B=%d messages have not been downloaded because they already existed locally" % msg_already_existing)
    print("Summary (local disc):")
    print("  C=%d messages are locally present which are also present on the server" % msg_keeping)
    print("  D=%d messages were moved to '%s'-directory because they are not present on server anymore" % (msg_move_to_deleted, deleted_folder))
    print("Consistency check: A+B==C; %d+%d==%d; %s" % (msg_downloaded,msg_already_existing,msg_keeping, 'SUCCESS' if msg_downloaded+msg_already_existing==msg_keeping else 'FAILED'))


def store_imap_to_mbox(imap4, directory) : 
    result, mailbox_list = imap4.list()
    if not result == 'OK' : raise RuntimeError('imap4.list(): ' + result) 

    from mailbox import mbox
    for mailbox in mailbox_list:
        (flags, delimiter, mailbox_name) = parse_list_response(mailbox.decode('utf-8'))
        
        mbox_file = mbox(directory + '/' + mailbox_name)

        result, data = imap4.select('"' + mailbox_name + '"', readonly=True)
        if not result == 'OK' : raise RuntimeError('imap4.select(' + mailbox_name + '): ' + result)

        result, data = imap4.uid('search', None, '(UNDELETED)' )
        if not result == 'OK' : raise RuntimeError("imap4.uid(search, ...) in " + mailbox_name + '): ' + result)

        for num in data[0].split():
            result, data = imap4.uid('fetch', num, '(RFC822)') # '(RFC822)' loads the whole message
            if not result == 'OK' : raise RuntimeError("imap4.uid(fetch, ...) in " + mailbox_name + '): ' + result) 

            mbox_file.add(data[0][1])

