# Standard library imports
import time
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional

# Third party imports

# Local imports

logger = logging.getLogger(__name__)

def send_email(
    sender_email: str,
    sender_password: str,
    to_emails: List[str],
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    smtp_server: str = 'smtp.gmail.com',
    smtp_port: int = 587,
    use_ssl: bool = False
) -> bool:
    """
    Sends an email via SMTP.

    Parameters
    ----------
        - sender_email (str): Email account that will send the email.
        - sender_password (str): Password of the email account.
        - to_emails (List[str]): List of primary recipient emails.
        - cc_emails (List[str]): Optional CC recipients.
        - bcc_emails (List[str]): Optional BCC recipients (blind).
        - subject (str): Optional Email subject.
        - body (str): Optional Plain text body.
        - smtp_server (str): SMTP server hostname, default is `smtp.gmail.com`.
        - smtp_port (str): SMTP port, default is 587.
        - use_ssl (): If True, use SMTP_SSL instead of starttls().

    Returns:
    -------
        True if sent successfully.

    Raises:
    ------
        ValueError: On invalid inputs.
    """
    
    if not to_emails:
        raise ValueError("Missing positional argument: 'to_emails' - at least one recipient is required.")

    if not sender_email:
        raise ValueError("Missing positional argument: 'sender_email'.")
    
    if not sender_password:
        raise ValueError("Missing positional argument: 'sender_password'.")

    all_recipients = to_emails + (cc_emails or []) + (bcc_emails or [])

    # Construct message
    msg = MIMEMultipart('alternative')
    msg['From'] = sender_email
    msg['To'] = ', '.join(to_emails)
    msg['Cc'] = ', '.join(cc_emails) if cc_emails else ''
    msg['Subject'] = subject

    # Attach bodies
    msg.attach(MIMEText(body, 'plain'))
    
    # TODO: handle HTML body

    # TODO: handle attachments

    # connect to email server and fire off email
    try:
        if use_ssl:
            ssl_smtp_port = 465
            server = smtplib.SMTP_SSL(smtp_server, ssl_smtp_port, timeout=10)

        else:
            server = smtplib.SMTP(smtp_server, smtp_port, timeout=10)
            server.starttls()

        server.login(sender_email, sender_password)
        server.sendmail(sender_email, all_recipients, msg.as_string())
        return True

    except smtplib.SMTPAuthenticationError as auth_err:
        error_msg = f"Authentication failed: {auth_err}"
        logger.error(error_msg)

    except smtplib.SMTPRecipientsRefused as recip_err:
        error_msg = f"Recipients refused: {recip_err}"
        logger.error(error_msg)

    except Exception as e:
        error_msg = f"Unexpected error sending email: {e}"
        logger.exception(error_msg)

    finally:
        server.quit()


def strava_import_email(
    success: bool,
    processed_activity_summary: str = None,
    new_activities_formatted: str = None,
    total_run_time: float = None,
    error_logs: str = None
) -> tuple[str, str]:
    """Returns an email subject and body for successful or failed import jobs"""
    
    now = time.strftime('%Y-%m-%d')
    
    if success:
        subject = f"Successful Strava Import 💯 // {now}"
        body = (
f"""Strava import ran successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}

{processed_activity_summary}

{new_activities_formatted}

Total runtime: {total_run_time} seconds 🚀"""
        )
    
    else:
        subject = f"Failed Strava Import 😭 // {now}"
        body = (
f"""The following errors were captured:

{error_logs}

Total runtime: {total_run_time} seconds"""
        )

    return subject, body