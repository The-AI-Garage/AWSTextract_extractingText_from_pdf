import json
import boto3
import os
import time
import logging
import sys

logger = logging.getLogger()
logger.setLevel("INFO")


class DocumentProcessor():
    jobId = ''
    region_name = ''

    roleArn = ''
    bucket = ''
    document = ''

    sqsQueueUrl = ''
    snsTopicArn = ''
    processType = ''
    
    def __init__(self, role, bucket, document, region):
        self.roleArn = role
        self.bucket = bucket
        self.document = document
        self.region_name = region
        
        # Instantiates necessary AWS clients
        session = boto3.Session()
        self.textract = session.client('textract', region_name= self.region_name)
        self.sqs = session.client('sqs', region_name=self.region_name)
        self.sns = session.client('sns', region_name=self.region_name)
    
    def CreateTopicandQueue(self):
        
        #millis = str(int(round(time.time() * 1000)))
        
        # create SNS topic
        sns_topic_Name = "AmazonTextractTopic"
        
        topic_response = self.sns.create_topic(Name = sns_topic_Name)
        self.snsTopicArn = topic_response['TopicArn']
        
        # create SQS queue
        sqs_Name = "AmazonTextractQueue"
        self.sqs.create_queue(QueueName = sqs_Name)
        self.sqsQueueUrl = self.sqs.get_queue_url(QueueName = sqs_Name)['QueueUrl']
        
        attribs = self.sqs.get_queue_attributes(QueueUrl = self.sqsQueueUrl, AttributeNames=['QueueArn'])['Attributes']
        sqs_arn = attribs['QueueArn']
        
        # Subscribe SQS queue to SNS topic
        self.sns.subscribe(TopicArn = self.snsTopicArn, Protocol = 'sqs', Endpoint = sqs_arn)
        
        # Authorize SNS to write SQS queue
        policy = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(sqs_arn, self.snsTopicArn)

        response = self.sqs.set_queue_attributes(QueueUrl = self.sqsQueueUrl, Attributes = {
            'Policy': policy
        })
    
    def DeleteTopicandQueue(self):
        self.sqs.delete_queue(QueueUrl = self.sqsQueueUrl)
        self.sns.delete_topic(TopicArn = self.snsTopicArn)
    
    def ProcessDocument(self):
        
        # Checks if job found
        job_found = False
        
        # Starts the text detection operation on the documents in the provided bucket
        # Sends status to supplied SNS topic arn
        response = self.textract.start_document_text_detection(
            DocumentLocation = {'S3Object': {'Bucket': self.bucket, 'Name': self.document}}, # The Amazon S3 bucket that contains the document to be processed. It's used by asynchronous operations.
            NotificationChannel = {'RoleArn': self.roleArn, 'SNSTopicArn': self.snsTopicArn} # To get the results of the text detection operation
            )
        logger.info('Processing type: Detection')
        
        logger.info('Start Job Id: ' + response['JobId'])
        dotline = 0
        
        while job_found == False:
            sqs_response = self.sqs.receive_message(
                QueueUrl=self.sqsQueueUrl, 
                MessageAttributeNames=['ALL'],
                MaxNumberOfMessages=10
                )
            
            # Waits until message are found in the SQS
            if sqs_response:
                if 'Messages' not in sqs_response:
                    if dotline < 40:
                        logger.info('.')
                    else:
                        logger.info('')
                        dotline = 0
                    sys.stdout.flush()
                    time.sleep(5)
                    continue
            
            # Checks for a completed job that matches the jobID in the response from
            # StartDocumentTextDetection
            for message in sqs_response['Messages']:
                notification = json.loads(message['Body'])
                textMessage = json.loads(notification['Message'])
                if str(textMessage['JobId']) == response['JobId']:
                    logger.info('Matching Job Found:' + textMessage['JobId'])
                    job_found = True
                    text_data = self.GetResults(textMessage['JobId'])
                    self.sqs.delete_message(
                        QueueUrl=self.sqsQueueUrl,
                        ReceiptHandle=message['ReceiptHandle']
                        )
                    return text_data
                else:
                    logger.info("Job didn't match:" +
                                str(textMessage['JobId']) + ' : ' + str(response['JobId']))
                # Delete the unknown message. Consider sending to dead letter queue
                self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                        ReceiptHandle=message['ReceiptHandle'])
        logger.info('Done!')
        
    # gets the results of the completed text detection job
    # checks for pagination tokens to determine if there are multiple pages in the input doc
    def GetResults(self, jobId):
        maxResults = 1000
        paginationToken = None
        finished = False
        detected_text = []

        while finished == False:
            response = None
            if paginationToken == None:
                response = self.textract.get_document_text_detection(JobId=jobId,
                                                                     MaxResults=maxResults)
            else:
                response = self.textract.get_document_text_detection(JobId=jobId,
                                                                    MaxResults=maxResults,
                                                                    NextToken=paginationToken)
            blocks = response['Blocks']
            
            # List to hold detected text
            #detected_text = []
            
            # Display block information and add detected text to list
            for block in blocks:
                if 'Text' in block and block['BlockType'] == "LINE":
                    detected_text.append(block['Text'])
            # If response contains a next token, update pagination token
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                finished = True
            
            #return detected_text
        return detected_text
