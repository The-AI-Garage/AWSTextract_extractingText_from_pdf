from detectFileAsync import DocumentProcessor
import boto3
import logging

logger = logging.getLogger()
logger.setLevel("INFO")

def create_sns_sqs(analyzer):
    logger.info("Create Topic and Queue")
    analyzer.CreateTopicandQueue()

def delete_sns_sqs(analyzer):
    logger.info("Deleting Topic and Queue")
    analyzer.DeleteTopicandQueue()

def process_document(roleArn, bucket, document, region_name):

    # Create analyzer class from DocumentProcessor, create a topic and queue, use Textract to get text,
    # then delete topica and queue
    logger.info("Init document processing")
    analyzer = DocumentProcessor(roleArn, bucket, document, region_name)
    logger.info("Create Topic and Queue")
    create_sns_sqs(analyzer)
    #logger.info("Create Topic and Queue")
    #analyzer.CreateTopicandQueue()
    logger.info("Extracting Text")
    extracted_text = analyzer.ProcessDocument()
    logger.info("Extracted Text: {}".format(extracted_text))
    #analyzer.DeleteTopicandQueue()
    
    logger.info("Lines in detected text:" + str(len(extracted_text)))
    
    # change document name
    analysis_results = str(document.replace("pdf","txt"))
    logger.info("document name: {}".format(analysis_results))
    # create txt 
    with open(f'/tmp/{analysis_results}', "w+") as f:
        # write result on txt
        for text in extracted_text:
            f.write(f'{text}\n')
    
    logger.info(len(extracted_text))
    return analysis_results, analyzer

def lambda_handler(event, lambda_context):

    # Initialize S3 client and set RoleArn, region name, and bucket name
    s3 = boto3.client("s3")
    roleArn = 'arn:aws:iam::013987100154:role/textractTest'
    region_name = 'us-east-1'
    bucket_name = 'bucket-textract-test007'

    # to hold all docs in bucket
    docs_list = []

    # loop through docs in bucket, get names of all docs
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    for bucket_object in bucket.objects.filter(Prefix='', Delimiter='/'):
        if bucket_object.key.endswith('.pdf'):
            docs_list.append(bucket_object.key)
    logger.info(docs_list)

    # For all the docs in the bucket, invoke document processing function,
    # add detected text to corpus of all text in batch docs,
    # and save CSV of comprehend analysis data and textract detected to S3
    for i in docs_list:
        analysis_results, analyzer = process_document(roleArn, bucket_name, i, region_name)
        name_of_file = str(analysis_results)
        object_key = f'simpleText_results/{name_of_file}'
        s3.upload_file(f'/tmp/{analysis_results}', bucket_name, object_key)
    
    # delete sns and sqs
    delete_sns_sqs(analyzer)