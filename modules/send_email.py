# Standard library imports
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional

# Third party imports

# Local imports


# configure logging at this module-level
logging.basicConfig(level=logging.INFO)
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
    Send an email via SMTP.

    Args:
        sender_credentials: Dict with 'email' and 'password' (or app token) keys.
        to_emails: List of primary recipient emails.
        subject: Email subject.
        body: Plain text body.
        smtp_server: SMTP server hostname.
        smtp_port: SMTP port.
        use_ssl: If True, use SMTP_SSL instead of starttls().
        cc_emails: Optional CC recipients.
        bcc_emails: Optional BCC recipients (blind).

    Returns:
        True if sent successfully.

    Raises:
        ValueError: On invalid inputs.

    Example:
        send_email(
            sender_credentials={'email': 'bot@example.com', 'password': 'app_pass'},
            to_emails=['recipient@example.com'],
            subject='Test',
            body='Hello',
        )
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
        logger.exception(error_msg)  # Logs traceback

    finally:
        server.quit()