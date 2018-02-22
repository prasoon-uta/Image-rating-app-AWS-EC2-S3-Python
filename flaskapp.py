import boto3
from flask import Flask, render_template, request, redirect, make_response
from flask import flash
import datetime
import os
import sys
import csv
import pymysql

#SQL connectivity
dbname = "pkdb"

conn = pymysql.Connect(host='', user='', passwd='', port=3306, local_infile=True, charset='utf8',
                     cursorclass=pymysql.cursors.DictCursor)

cursor = conn.cursor()
cursor.execute("create database if not exists pkdb")

app = Flask(__name__)
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])
app.secret_key = ''


aid = ''
apwd = ''

bucket_name=''
@app.route('/')
def hello_world():

        return render_template('login.html')


@app.route('/login',methods=['GET','POST'])
def login():
        user = request.form['username']


        listoflists = viewdata()
        return render_template('file-upload.htm', msg=user, other=listoflists)




def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

#------------------------------------- Delete File ---------------------------------
@app.route('/DeleteFile',methods=['GET','POST'])
def DeleteFile():
        return render_template('file-delete.htm')

@app.route('/delfile',methods=['POST'])
def delfile():
        filename=request.form['fileName']
        s3 = boto3.resource('s3',aws_access_key_id=aid,aws_secret_access_key=apwd)
        s3.Object(bucket_name, filename).delete()
        return 'File deleted'

#-----------------------------------File Upload ------------------------------
@app.route('/file-upload', methods=['GET', 'POST'])
def fileUpload():

    return render_template('file-upload.htm')

@app.route('/home', methods=['GET', 'POST'])
def home():

    return render_template('login.html')


@app.route('/save-file', methods=['GET','POST'])
def saveFile():
        #print 'save'
        username = request.form['hidden_user_val']

        listoflists1=viewdata()
        file = request.files['file']
        isvalid = allowed_file(file.filename)
        #if not isvalid:
        #       return render_template('file-upload.htm', msg=username,errormsg="Invalid extension", other=listoflists1)


        title = request.form['title']
        bucket_name = 'prasoon-bucket'
        now = datetime.datetime.now().date()
        rating = 0

        #Create a bucket a save the image, get the address to save in Database

        s3 = boto3.resource('s3',aws_access_key_id=aid,aws_secret_access_key=apwd)
        bucket = s3.create_bucket(Bucket = bucket_name)
        result = bucket.put_object(Key=file.filename, Body=file.stream.read())

        url = '{}/{}/{}'.format('https://s3.amazonaws.com', bucket_name ,file.filename)

        #image = read_file("C:\\Users\\praso\\Desktop\\quiz_file\\" + file.filename)

        add_image = "INSERT INTO "+dbname+"."+"pktable "+"(image_title,image_owner,image_name,image_addr,created_date,rating) VALUES (%s, %s, %s, %s, %s, %s);"
        data_image = (title, username, file.filename, url,now, int(rating))
        cursor.execute(add_image, data_image)
        conn.commit()

        #reload with all the images
        listoflists = viewdata()
        return render_template('file-upload.htm', msg=username, other=listoflists)


#----------------------- List Files -------------------------------
@app.route('/ListFiles',methods=['GET','POST'])
def ListFiles():
        html = ''
        s3 = boto3.resource('s3',aws_access_key_id=aid,aws_secret_access_key=apwd)
        temp = []
        bucket=s3.Bucket(bucket_name)
        for obj in bucket.objects.all():
                temp.append(obj.key)
        html = '''<html>
                       <title>Attributes</title>
                       %s
               </html>''' %(temp)
        return html

#------------------File Download ---------------------------------
@app.route('/fileDownload',methods=['GET','POST'])
def fileDownload():
        return render_template('file-download.htm')

@app.route('/downloadfile',methods=['POST'])
def downloadfile():
        filename=request.form['fileName']
        s3 = boto3.resource('s3',aws_access_key_id=aid,aws_secret_access_key=apwd)
        fContent=s3.Object(bucket_name, filename).get()['Body'].read()
        response = make_response(fContent)
        response.headers["Content-Disposition"] = "attachment; filename="+filename+";"
        return response

#update file


@app.route('/update-file', methods=['GET','POST'])
def updateFile():
        #print 'save'
        username = request.form['hidden_user_val']
        id = request.form['id']

        now = datetime.datetime.now().date()
        rating = request.form['rating']

        query = "UPDATE pkdb.pktable SET total_count = total_count + %s,total_sum = total_sum + %s  WHERE ID = %s "
        data = (1,rating,id)
        cursor.execute(query, data)

        query = "select  total_count,total_sum  from " + dbname + "." + "pktable where ID = %s"
        data = id
        cursor.execute(query,data)
        result = cursor.fetchone()

        new_rating = result['total_sum']/result['total_count']

        query = "UPDATE pkdb.pktable SET created_date = %s, rating = %s  WHERE ID = %s "

        data = (now,new_rating,id)
        cursor.execute(query,data)

        conn.commit()
        listoflists=viewdata()
        return render_template('file-upload.htm', msg=username, other=listoflists)


def viewdata():
        # reload with all the images
        query = "select  id,image_addr,image_title,rating,created_date from " + dbname + "." + "pktable"
        cursor.execute(query)
        data = cursor.fetchall()
        listoflists = []

        for row in data:
                images = []
                images.append(row['id'])

                images.append(row['image_title'])
                images.append(row['rating'])
                images.append(row['image_addr'])
                images.append(row['created_date'])
                listoflists.append(images)

        return listoflists


if __name__ == '__main__':
        app.run()
