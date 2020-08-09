#!/usr/bin/env python
# coding: utf-8

"""
Convenience API to Mailgun

Created: 11.05.20
"""

import requests
import logging

import config


def shoot_mail(subject: str, body: str) -> None:
    url = config.get("mailgun", "url")
    auth_key = config.get("mailgun", "auth-key")
    from_ = config.get("mailgun", "from")
    to = config.get("mailgun", "to")
    active = url and auth_key and from_ and to

    if not active:
        logging.info(f"no mailgun account configured (subject={subject})")
    else:
        logging.info(f"sending mail: {subject}")
        try:
            r = requests.post(
                url,
                auth=("api", auth_key),
                data={
                    "from": from_,
                    "to": to,
                    "subject": subject,
                    "text": body
                })
            logging.info(f"mailgun: HTTP {r.status_code}")
        except Exception as ex:
            logging.exception(f"mailgun")

