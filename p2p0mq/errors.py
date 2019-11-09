# -*- coding: utf-8 -*-
"""
"""
from __future__ import unicode_literals
from __future__ import print_function

class BaseError(Exception):
    pass


class ValidationError(BaseError):
    """ The caller failed to pass the restrictions imposed by callee. """
    pass


class MessageValidationError(ValidationError):
    """ The message cannot be decoded because required fields are missing
    or objects have improper type. """
    pass


class MissingCertificateError(object):
    """ We were asked to connect to a peer but we don't have its
    certificate. """
    pass
