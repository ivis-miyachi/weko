# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""OpenAIRE service integration for Invenio repositories."""

from __future__ import absolute_import, print_function

from datetime import datetime

from celery import shared_task
from flask import current_app
from invenio_db import db

from .models import Community, InclusionRequest
from weko_handle.api import Handle


# @shared_task(ignore_result=True)
# def delete_marked_communities():
#     """Delete communities after holdout time."""
#     # TODO: Delete the community ID from all records metadata first
#     raise NotImplementedError()
#     Community.query.filter_by(
#         Community.delete_time > datetime.utcnow()).delete()
#     db.session.commit()


@shared_task(ignore_result=True)
def delete_expired_requests():
    """Delete expired inclusion requests."""
    try:
        InclusionRequest.query.filter(
            InclusionRequest.expires_at > datetime.utcnow()).delete()
        db.session.commit()
    except Exception as ex:
        db.session.rollback()

@shared_task(ignore_result=True)
def delete_handle(hdl):
        weko_handle = Handle()
        handle = weko_handle.delete_handle(hdl)
        if handle:
            current_app.logger.info(hdl + ' handle deleted successfully.')
        else:
            current_app.logger.info( hdl + " handle delete failed.")
