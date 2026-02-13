import smtplib
from email.message import EmailMessage

from dotenv import load_dotenv
from os import environ


def send_email(msg_to, msg_txt):
        
    load_dotenv()
    email_address = environ['EMAIL_ADDRESS']
    email_password = environ['EMAIL_PASSWORD']
    msg = EmailMessage()
    msg.set_content(msg_txt)
    msg["Subject"] = "Данные по студентам"
    msg["From"] = email_address
    msg["To"] = msg_to
    with smtplib.SMTP_SSL("smtp.mail.ru", 465) as server:
        server.login(email_address, email_password)
        server.send_message(msg)
