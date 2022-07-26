import pymongo
from flask import Flask, json, flash, redirect, request, jsonify, make_response
from flask_mongoengine import MongoEngine
from datetime import datetime
from google.cloud import storage
from jiltocsv import JILtoCSV
import os
import re
import csv
import urllib.request
from werkzeug.utils import secure_filename

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'synthetic-eon-241312-35b648071a59.json'

# storage_client = storage.Client()

bucket_name1 = 'uploading_files_test'
destination_blob_name1 = 'file_uploaded'
file_path = r'C:\localjilfiles'
myList = []

app = Flask(__name__)

app.config['MONGODB_SETTINGS'] = {
    'db': 'dbmongocrud',
    'host': 'localhost',
    'port': 27017
}
db = MongoEngine()
db.init_app(app)

# UPLOAD_FOLDER = 'C:\localjilfiles'
# DOWNLOAD_PATH = r'C:\localjilfiles'
# app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# app.config['DOWNLOAD_PATH'] = DOWNLOAD_PATH
# app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jil', 'jpeg', 'gif'])


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


class Status(db.DynamicEmbeddedDocument):
    stateFileName = db.StringField()
    time = db.DateTimeField()
    state = db.StringField()
    blobUrl = db.StringField()


# Dict = {"stateFileName"," ","time","","state","","blobUrl",""}


class User(db.Document):
    name = db.StringField()
    stages = db.EmbeddedDocumentListField(Status)
    # stateFileName = db.StringField()
    time = db.DateTimeField()
    status = db.StringField()
    state = db.StringField()
    # time = db.DateField()


@app.route('/')
def main():
    return 'Homepage'


@app.route('/getAllFiles', methods=['GET'])
def get_files():
    files = User.objects()
    return jsonify(files), 200


def upload_blob(destination_blob_name, source_file_name, bucket_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        # blob = bucket.blob(source_file_name)
        # fileName = source_file_name.rsplit('/')[-1]
        # print(fileName)
        stats = storage.Blob(bucket=bucket, name=destination_blob_name).exists(storage_client)
        print(stats)

        if not stats:
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)
            print(
                "File {} uploaded to Storage Bucket with Bob name  {} successfully .".format(
                    source_file_name, destination_blob_name
                )
            )
            return True
        else:
            print("File already Existed")
            return False

    except Exception as e:
        print(e)
        return False


def download_blob(bucket_name, source_blob_name, destination_file_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_path)

        print(
            "Blob {} downloaded to file path {}. successfully ".format(
                source_blob_name, destination_file_path
            )
        )
        status = Status(stateFileName=source_blob_name, time=datetime.now(), state="Downloaded jilFile Successfully",
                        blobUrl="dcc")
        stages = {"stage2": status}
        for user in User.objects(name=source_blob_name):
            myList = user.stages
            print(user.name)

        myList.append(stages)
        user.update(stages=myList)

        # response = User(name=source_blob_name, status="Downloaded jilFile Successfully", time=datetime.now(),
        # state="JilFile")
        # response.save()
        return True
    except Exception as e:
        print(e)
        return False


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'files[]' not in request.files:
        resp = jsonify({'message': 'No file part in the request'})
        resp.status_code = 400
        return resp

    files = request.files.getlist('files[]')
    # args = request.args
    # destination_blob_name1 = args.get('name')
    errors = {}
    success = False

    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            print(filename)
            # file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            uploadedStatus = upload_blob(filename, os.path.join(file_path, filename), bucket_name1)
            if uploadedStatus:
                status = Status(stateFileName=filename, time=datetime.now(), state="uploaded jilFile Successfully",
                                blobUrl="dcc")
                stages = {"stage1": status}
                myList.append(stages)
                response = User(name=filename, stages=myList)
                response.save()
                success = True
            else:
                responseBody = {"message": "File already Existed"}
                return make_response(jsonify(responseBody), 200)

        else:
            errors[file.filename] = 'File type is not allowed'

    if success:
        resp = jsonify(response)
        resp.status_code = 201
        return resp
    else:
        resp = jsonify(errors)
        resp.status_code = 500
        return resp


@app.route('/convertJilfile', methods=['POST'])
def convert_files():
    errors = {}
    args = request.args
    source_file_name = args.get('source_file_name')

    success = download_blob(bucket_name1, source_file_name, 'C:\localjilfiles\{}'.format(source_file_name))

    csv_file = JILtoCSV(f'C:\localjilfiles\{source_file_name}')

    # dag_csvFile= CSVtoDAG(csv_file)

    # dag_file= DAGCSVtoDAG(dag_csvFile)

    renamedFile = source_file_name.replace("jil", "csv")
    status = Status(stateFileName=renamedFile, time=datetime.now(),
                    state="Converted from JilFile to JilCsv Successfully", blobUrl="dcc")
    stages = {"stage3": status}
    for user in User.objects(name=source_file_name):
        myList = user.stages
        print(user.name)

    myList.append(stages)
    user.update(stages=myList)
    upload_blob(renamedFile, csv_file, bucket_name1)
    # response = User(name=source_file_name, status="uploaded jilcsv Successfully", time=datetime.now(), state="cvsFile")
    # response.save()

    if success:
        resp = jsonify({'message': 'Dag File uploaded successfully'})
        resp.status_code = 201
        return resp
    else:
        resp = jsonify(errors)
        resp.status_code = 500
        return resp


if __name__ == '__main__':
    app.run(debug=True)
