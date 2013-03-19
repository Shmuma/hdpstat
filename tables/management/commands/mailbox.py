"""
Export hbase tables history from mailbox
"""

import imaplib
import sys
import netrc
import os

from django.core.management.base import BaseCommand


class Command (BaseCommand):
    host = "imap.mail.ru"
    dest = "out"

    def handle (self, *args, **options):
        nrc = netrc.netrc ()
        auths = nrc.authenticators (self.host)

        if auths == None:
            print "No authentication for host '%s'" % self.host

        imap = imaplib.IMAP4_SSL (self.host)

        try:
            imap.login (auths[0], auths[2])
            imap.select ('Hadoop', False)
            typ, data = imap.search (None, 'ALL')
            for num in data[0].split ():
                typ, data = imap.fetch (num, '(RFC822)')
                print num
                with open (os.path.join (self.dest, "%s.txt" % num), "w+") as fd:
                    fd.write (data[0][1])
            imap.close ()
            imap.logout ()
        except imap.error as e:
            print "Error: %s"  % e
