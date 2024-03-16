# -*- coding: utf-8 -*-

import sys
import smtplib, time, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

def send_mail(level, Subject, content, receivers):
    print("Send Mail......")

    # Define SMTP email server details
    smtp_server = 'email-smtp.us-east-1.amazonaws.com'
    smtp_port = 25
    sender_username = 'AKIA35FGARBHIMKYBKNG'
    sender_password = 'BJxmT319pPzheSU0v8RicbRsomyZb19xUpuCdxQOnFXJ'

    # Construct email
    # receivers = ['yadong.zhu@mobvista.com','jun.wan@mobvista.com','shi.yang@mobvista.com','guangxue.yin@mobvista.com','yang.xiao@mobvista.com','xujian@mobvista.com', 'zhaocheng.liu@mobvista.com','nanqi.huang@mobvista.com','hai.bai@mintegral.com','xiaoyi.zhang@mobvista.com','liduo@mintegral.com']
    msg = MIMEMultipart()
    msg['From'] = 'model_team_monitor@mobvista.com'
    msg['To'] = receivers
    msg['Subject'] = '[%s] %s' % (level, Subject)
    msg["Accept-Charset"]="ISO-8859-1,utf-8"
    context=MIMEText(content)
    msg.attach(context)

    # attactment=MIMEApplication(open(ff, 'rb').read())
    # attactment.add_header('Content-Disposition', 'attachment', filename=ff)
    # msg.attach(attactment)

    # Send email
    s = smtplib.SMTP(smtp_server, smtp_port)
    s.starttls()
    s.login(sender_username, sender_password)
    print("level: %s" % level)
    print("subject: %s" % Subject)
    print("content: %s" % content)
    print("receivers: %s" % receivers)
    s.sendmail(msg['From'], receivers.split(","), msg.as_string())
    #s.sendmail(msg['From'], msg['To'], msg.as_string())
    s.quit()
    print("Send successfully......")


if __name__ == "__main__":

    level="INFO"
    Subject=sys.argv[1]
    content = ""
    receivers = "hai.bai@mintegral.com"

    send_mail(level, Subject, content, receivers)