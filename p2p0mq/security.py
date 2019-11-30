# -*- coding: utf-8 -*-
"""
Security related settings and actions are grouped in a
distinct class. Current implementation merges the manager into
the :ref:`application class<top_level_management>`.

The security manager is initialized using :meth:`~SecurityManager.start_auth`
which is a no-op if :py:attr:`~SecurityManager.no_encryption` is `True`.
It's main role is to start the
`authenticator thread <https://pyzmq.readthedocs.io/en/latest/api/zmq.auth.thread.html>`_.
While :meth:`~SecurityManager.start_auth` will initialize the thread to read
existing certificates in :py:attr:`~SecurityManager.public_cert_dir`, please
note that any certificates added later will not be acknowledged until
`SecurityManager.auth_thread.configure_curve() <https://pyzmq.readthedocs.io/en/latest/api/zmq.auth.thread.html#zmq.auth.thread.ThreadAuthenticator.configure_curve>`_
gets called.

Terminating the manager through :meth:`~SecurityManager.terminate_auth`
will stop the  authenticator thread, if any.

Cert Store
----------

Certificates on the file system are organized in a "store". Public
directory stores the certificates of other peers while the private directory
contains certificates of peers running on the local machine.

The names of the certificates are always the same as the uuid of the peer,
with the extension being either `key` for public certificates or
`key_secret` for private certificates.

The class implements some helper methods for dealing with the certificates store:

* :meth:`~SecurityManager.prepare_cert_store` creates the directory structure \
and uses
* :meth:`~SecurityManager.cert_pair_check_gen` to either \
read or create certificates for current peer;
* :meth:`~SecurityManager.cert_file_by_uuid` will compute the path towards \
the certificate corresponding to that uuid; it is used in
* :meth:`~SecurityManager.cert_key_by_uuid` which parses the content of the \
file and reads the key;
* :meth:`~SecurityManager.exchange_certificates` is mostly of use for testing \
but offers some hints about what the user should do to securely connect \
two peers using certificates.

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

    Attributes:
        private_cert_dir (str):
            The path towards the directory that stores private certificates.
        public_cert_dir (str):
            The path towards the directory that stores public certificates.
        temp_cert_dir (str):
            The path towards the temporary directory (used for generating
            new certificates). If not provided, it defaults to system's
            temporary directory.
        no_encryption (bool):
            Enable or disable encryption at peer level. Peers that use
            encryption can only connect to other peers that use encryption
            and vice-versa.
        public_file (str):
            The path towards the public certificate of this peer.
        private_file (str):
            The path towards the private certificate of this peer.
        auth_thread (ThreadAuthenticator):
            A separate thread used by zmq to authenticate our peers.

    """
    def __init__(self,
                 private_cert_dir, public_cert_dir,
                 temp_cert_dir=None,
                 no_encryption=False,
                 *args, **kwargs):
        """
        Constructor.

        Arguments:
            private_cert_dir (str):
                The path towards the directory that stores private
                certificates.
            public_cert_dir (str):
                The path towards the directory that stores public certificates.
            temp_cert_dir (str):
                The path towards the temporary directory (used for generating
                new certificates). Defaults to system's temporary directory.
            no_encryption (bool):
                Enable or disable encryption at peer level. Peers that use
                encryption can only connect to other peers that use encryption
                and vice-versa.
        """
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
        """
        Starts the authentication thread if encryption is enabled.

        Arguments:
            context (zmq.Context):
                The context where the authentication thread will belong.
        """
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
        might not be fully set (an exception in
        :meth:`p2p0mq.app.theapp.TheApp.create` does not prevent
        this method from being executed).
        """
        if self.auth_thread is not None:
            logger.debug("Authenticator thread is being stopped")
            self.auth_thread.stop()
            self.auth_thread = None

    def prepare_cert_store(self, uuid):
        """
        Prepares the directory structure before it can
        be used by our authentication system.

        Arguments:
            uuid:
                The unique identification of local peer.
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
        """
        Checks if the certificates exist. Generates them if they don't.

        Arguments:
            uuid:
                The unique identification of the peer. Usually, this is
                the local peer.
        """
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

        Arguments:
            uuid:
                The unique identification of the peer. Usually, this is
                the local peer.
            public (bool):
                If True it retrieves the path of the public certificate,
                if False it retrieves the path of the private certificate.
        """
        if isinstance(uuid, bytes):
            uuid = uuid.decode('utf-8')
        pb_vs_pv = 'key' if public else 'key_secret'
        return os.path.join(
            self.public_cert_dir if public else self.private_cert_dir,
            '%s.%s' % (uuid, pb_vs_pv)
        )

    def cert_key_by_uuid(self, uuid, public=True):
        """
        Reads the key from corresponding certificate file.

        Arguments:
            uuid:
                The unique identification of the peer. Usually, this is
                the local peer.
            public (bool):
                If True it retrieves the public key,
                if False it retrieves the private key.
        """
        file = self.cert_file_by_uuid(uuid=uuid, public=public)
        logger.debug("%s certificate for uuid %s is loaded from %s",
                     'Public' if public else 'Private',
                     uuid, file)
        if not os.path.exists(file):
            return None
        public_key, secret_key = zmq.auth.load_certificate(file)
        return public_key if public else secret_key

    def exchange_certificates(self, other):
        """
        Copies the certificates so that the two instances can
        authenticate themselves to each other.

        Arguments:
            other (SecurityManager):
                The security manager of the other peer.
        """
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
        self.auth_thread.configure_curve(
            domain='*', location=self.public_cert_dir)
        other.auth_thread.configure_curve(
            domain='*', location=other.public_cert_dir)
