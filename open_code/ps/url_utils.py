def use_s3(url):
    return url.replace('s3a://', 's3://')

def use_s3a(url):
    return url.replace('s3://', 's3a://')
