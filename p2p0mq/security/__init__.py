# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

import logging
import os
import re
import shutil
import tempfile
from time import time

import zmq
from zmq.auth.thread import ThreadAuthenticator

logger = logging.getLogger('p2p0mq.sec')


class SecurityManager(object):
    """
    Manages the security related settings and actions.
    """
    def __init__(self,
                 private_cert_dir, public_cert_dir,
                 temp_cert_dir=None,
                 no_encryption=False,
                 *args, **kwargs):
        """ Constructor. """
        super(SecurityManager, self).__init__(*args, **kwargs)
        self.private_cert_dir = private_cert_dir
        self.public_cert_dir = public_cert_dir
        if temp_cert_dir is None:
            self.temp_cert_dir = tempfile.gettempdir()
        else:
            self.temp_cert_dir = temp_cert_dir

        # Paths for certificates used by this instance.
        self.public_file = None
        self.private_file = None

        # Security settings.
        self.no_encryption = no_encryption

        # Authentication thread.
        self.auth_thread = None

    def start_auth(self, context):
        """ Starts the authentication thread if encryption is enabled. """
        if not self.no_encryption:
            logger.debug("Authenticator thread is being started")
            self.auth_thread = ThreadAuthenticator(
                context=context, encoding='utf-8',
                log=logging.getLogger('zmq_auth')
            )
            self.auth_thread.start()
            self.auth_thread.thread.name = 'zmq_auth'

            self.auth_thread.configure_curve(
                domain='*', location=self.public_cert_dir)

    def terminate_auth(self):
        """
        Ends the authentication thread if encryption is enabled.

        This method should be written defensively, as the environment
        might not be fully set (an exception in create() does not prevent
        this method from being executed).
        """
        if self.auth_thread is not None:
            self.auth_thread.stop()
            self.auth_thread = None

    def prepare_cert_store(self, uuid):
        """
        Prepares the directory structure before it can
        be used by our authentication system.
        """

        if not os.path.isdir(self.private_cert_dir):
            os.makedirs(self.private_cert_dir)
        if not os.path.isdir(self.public_cert_dir):
            os.makedirs(self.public_cert_dir)
        if not os.path.isdir(self.temp_cert_dir):
            os.makedirs(self.temp_cert_dir)

        self.public_file, self.private_file = \
            self.cert_pair_check_gen(uuid)

    def cert_pair_check_gen(self, uuid):
        """ Checks if the certificates exist. Generates them if they don't. """
        cert_pub = self.cert_file_by_uuid(uuid, public=True)
        cert_prv = self.cert_file_by_uuid(uuid, public=False)
        pub_exists = os.path.isfile(cert_pub)
        prv_exists = os.path.isfile(cert_prv)

        if pub_exists and prv_exists:
            # Both files exist. Yey.
            pass
        elif pub_exists and not prv_exists:
            # The public certificate exists but is unusable without
            # the private one.
            raise RuntimeError("The public certificate has been found at %s, "
                               "which indicates that a key has been generated, "
                               "but the private certificate is not at %s",
                               cert_pub, cert_prv)
        elif not pub_exists and prv_exists:
            # The private certificate exists but the public one doesn't.
            # We can extract the key from the private one.
            with open(cert_prv, 'r') as fin:
                data = re.sub(r'.*private-key = "(.+)"', "", fin.read(),
                              re.MULTILINE)
            with open(cert_pub, 'w') as fout:
                fout.write(data)
        else:
            # Neither exists.
            public_file, secret_file = \
                zmq.auth.create_certificates(
                    self.temp_cert_dir,
                    '%r' % time())
            shutil.move(public_file, cert_pub)
            shutil.move(secret_file, cert_prv)
        return cert_pub, cert_prv

    def cert_file_by_uuid(self, uuid, public=True):
        """
        Computes the path of a certificate inside the certificate store
        based on the name of the peer.
        """
        if isinstance(uuid, bytes):
            uuid = uuid.decode('utf-8')
        pb_vs_pv = 'key' if public else 'key_secret'
        return os.path.join(
            self.public_cert_dir if public else self.private_cert_dir,
            '%s.%s' % (uuid, pb_vs_pv)
        )

    def cert_key_by_uuid(self, uuid, public=True):
        """ Reads the key from corresponding certificate file. """
        file = self.cert_file_by_uuid(uuid=uuid, public=public)
        logger.debug("%s certificate for uuid %s is loaded from %s",
                     'Public' if public else 'Private',
                     uuid, file)
        if not os.path.exists(file):
            return None
        public_key, secret_key = zmq.auth.load_certificate(file)
        return public_key if public else secret_key

    def exchange_certificates(self, other):
        """ Copies the certificates so that the two instances can
        authenticate themselves to each other. """
        shutil.copy(
            self.public_file,
            os.path.join(other.public_cert_dir,
                         os.path.basename(self.public_file))
        )
        shutil.copy(
            self.public_file,
            os.path.join(other.public_cert_dir,
                         os.path.basename(self.public_file))
        )
        shutil.copy(
            other.public_file,
            os.path.join(self.public_cert_dir,
                         os.path.basename(other.public_file))
        )
        shutil.copy(
            other.public_file,
            os.path.join(self.public_cert_dir,
                         os.path.basename(other.public_file))
        )
        self.auth_thread.configure_curve(
            domain='*', location=self.public_cert_dir)
        other.auth_thread.configure_curve(
            domain='*', location=other.public_cert_dir)
