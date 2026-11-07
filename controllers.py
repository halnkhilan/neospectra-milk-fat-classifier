from app import app
import pandas as pd
import urllib.request
import numpy as np
from flask_sqlalchemy import SQLAlchemy
import io
from flask import Flask, render_template, request, flash,  redirect, url_for, Response, session, abort
import time
import paho.mqtt.client as paho
from paho import mqtt
import threading
import os
from sqlalchemy import create_engine, exc
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from io import BytesIO
import base64
from io import StringIO
import matplotlib.pyplot as plt
import csv 
#import execjs
from flask import get_flashed_messages
import ssl
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView


app.secret_key = "super secret key"


from app.src import milk
# Model Loading
RETRAIN = False
model = milk.Predict(reTrain=RETRAIN)

cwd = os.path.dirname(__file__)
app.config['UPLOAD_FOLDER'] = os.path.join(cwd, 'uploads')
app.config['IMG_FOLDER'] = os.path.join(cwd, 'static')
app.config['DEC_FOLDER'] = os.path.join(cwd, 'decrypt')
app.config['CERT_FOLDER'] = os.path.join(cwd, 'cert')
#Database 

admin= Admin(app) 

uploadFile = False
swready = False
swreadyMessage = ""
Client_Name = ""


params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SQL5053.site4now.net;DATABASE=db_a8abea_neospectradb;Uid=db_a8abea_neospectradb_admin;PWD=123456Nm;Trusted_Connection=no;')
Database_con = app.config['SQLALCHEMY_DATABASE_URI'] = "mssql+pyodbc:///?odbc_connect=%s" % params
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False 

db=SQLAlchemy(app)
engine=create_engine(Database_con) 


con=engine.connect()

#########################

#filePath1 = os.path.join(app.config['DEC_FOLDER'], 'toaster.js')
#ctx = execjs.compile(window.location.reload())

##########33


def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

strMessage=""

# it should be changes according to list value 

client = paho.Client(client_id="6", userdata=None, clean_session=False ,protocol=paho.MQTTv311) 

client.on_connect = on_connect

# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLSv1_2)
# set username and password
client.username_pw_set("Hive123456", "Hive123456")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect("c5dc9b9a64074b5e9041ef99cc5e47fd.s1.eu.hivemq.cloud", 8883)
# setting callbacks for different events to see if it works, print the message etc.
#client.tls_set(ca_path, certfile = ca_cert, keyfile=ca_key , cert_reqs = ssl.CERT_REQUIRED, tls_version = ssl.PROTOCOL_TLSv1_2 , ciphers=None)
# set username and password
#client.username_pw_set("Hive123456", "Hive123456")
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
#client.connect("anmq6jiydxx9z-ats.iot.us-east-1.amazonaws.com", 8883)
# setting callbacks for different events to see if it works, print the message etc.

# with this callback you can see if your publish was successful
def on_publish(client, userdata, mid, properties=None):
    print("mid: " + str(mid))

# print which topic was subscribed to
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

# print message, useful for checking if it was successful
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + msg.payload.decode("utf-8") )
    #redirect(url_for('predict', clientname=msg.topic , clientmessage=str(msg.payload)) )
    global strMessage
    global Client_Name
    global swready
    global swreadyMessage
    pay = msg.topic
    if pay == "client/sw":
     swready = True
     swreadyMessage = "The client "+msg.payload.decode("utf-8") +" is ready"
     #ctx.call("success")
     with app.app_context(), app.test_request_context():
      flash(swreadyMessage)
      clientlist=con.execute("SELECT Client_ID, Client_name FROM[dbo].[Client]")
      return render_template("mainpage.html",clientlist=clientlist )  
     #execjs.eval("window.location.href = window.location.href") 
   # execjs.eval("toastr.success('rrr')")
    #strMessage = "the client name (" +msg.topic+") sends message ("+msg.payload.decode("utf-8") +") by using MQTT protocol " 
    
    if len(Client_Name) != 0 :
     if pay == "client/"+Client_Name['Client_name'].iloc[0]+"/file":
      pay = ""
      uploadFile1(msg.payload)
     if pay == "client/"+Client_Name['Client_name'].iloc[0]+"/background":
      with app.test_client() as client:
       flash("The background has been activated")
       client.get('/mainpage')
       
       #clientlist=con.execute("SELECT Client_ID, Client_name FROM[dbo].[Client]")
       #return render_template("mainpage.html",clientlist=clientlist )  

@app.teardown_request
def show_teardown(exception):
    flash("The background has been activated")
    print('after with block')

def uploadFile1(msg):
    
    filePath = os.path.join(app.config['UPLOAD_FOLDER'], 'read.txt')
    f = open(filePath, mode = 'w', newline='')
    f.write(msg.decode("utf-8"))
    f.close()
    stream = io.StringIO(msg.decode("UTF8"), newline=None)
    ef = pd.read_csv(stream, sep='\t')
    print(ef)
    global strMessage
    strMessage = "the milk sample belongs to " +model.predictSample(df=ef)
    #client_ID= 18
    global Client_Name
    global uploadFile
    uploadFile = True
    clientlist=pd.read_sql_query("SELECT Client_ID FROM[dbo].[Client] where Client_name = '"+str(Client_Name['Client_name'].iloc[0])+"'",con)
    print("clientlist")
    print(clientlist)
    client_ID= int(clientlist['Client_ID'].iloc[0])
    print("client_ID")
    print(client_ID)
    f = open(filePath, 'r')
    con.execute("INSERT INTO [dbo].[Data](Measuerment_txt,Classification_result,Client_ID) VALUES ('"+msg.decode("UTF8")+"','"+model.predictSample(df=ef)+""+"','"+str(client_ID)+"');")
    f.close()
    


def plot_png(filename):
    Xx = []
    Yy = []
    filePath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    with open(filePath, 'r') as datafile:
        plotting = csv.reader(datafile, delimiter='\t')
        print(type(plotting))
        ddd = list(plotting)
        del ddd[0]
        print(ddd)
        count = 1
        for ROWS in ddd:
         if count < 141:
          print(ROWS)
          Xx.append(float(ROWS [0]))
          Yy.append(float(ROWS [1]))
          count += 1
    fig = Figure()
    img = BytesIO()
    axis = fig.add_subplot(1, 1, 1)
    axis.plot(Xx, Yy)
    axis.set_xlabel("Wavelength (nm)")
    axis.set_ylabel("Reflectance or Transmittance")
    axis.legend(['Milk Spectrometry'], loc ="lower right")
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    plt.savefig(output, format='png')
    plt.close()
    img.seek(0)
    plot_url = base64.b64encode(output.getvalue()).decode('utf8')
    
    #with app.app_context(), app.test_request_context():
    #return Response(output.getvalue(), mimetype='image/png')
    
    #return render_template('plot.html', image = plot_url )
    return  plot_url

class SecureModelView(ModelView):
    def is_accessible(self):
        if 'logged_in' in session:
          return True
          
        else:
          abort(403)

class User(db.Model):
    User_ID = db.Column(db.Integer, primary_key=True, autoincrement=True)
    User_Name = db.Column('User_Name',db.String(100),nullable=True)
    User_Email = db.Column('User_Email',db.String(100),nullable=True)
    User_Password = db.Column('User_Password',db.String(100),nullable=True)
    
    def __init__(self,User_Name,User_Email,User_Password):
      self.User_Name=User_Name
      self.User_Email= User_Email
      self.User_Password=User_Password

admin.add_view(ModelView(User, db.session))

@app.route('/', methods = ['GET', 'POST'])    
def homePage(): 
      connectionMqtt()
      return render_template('loginPage.html')

@app.route('/login', methods = ['GET', 'POST'])
def login():
   
    if request.method == 'POST':
       UserName = request.form.get("username")
       Password = request.form.get("password")
       type = request.form.get("type")
       print(type)


       if type == "admin":
         adminloginlist= pd.read_sql_query("SELECT Admin_Email, Admin_password FROM[dbo].[Admin] WHERE Admin_Email='"+UserName+"' AND Admin_password='"+Password+"'",con)
         adminloginlist=adminloginlist.values
         #adminName = "batool@kacst.edu.sa"
         #pass1 = "123456"

         if (len(adminloginlist) == 1):
           return redirect(url_for('home'))
         else:
           flash('Username or Password is incorrect ')
           return redirect(url_for('homePage'))

       else:
         print(Password)
         userloginlist= pd.read_sql_query("SELECT User_Name, User_Password FROM[dbo].[User] WHERE User_Email='"+UserName+"' AND User_Password='"+Password+"'",con)
         userloginlist=userloginlist.values
       # sql_select_Query="SELECT User_Name, User_Password FROM[dbo].[User] WHERE User_Name='"+UserName+"' AND User_Password ='"+Password+"';"
       # rows=db.execute(sql_select_Query)
       # cur = con.cursor()
       # rows=cur.execute(sql_select_Query)
       # rows= rows.fetchall()
       
         if (len(userloginlist) == 1):
          session['logged_in']= True
          return redirect(url_for('mainpage'))
         else:
          flash('Username or Password is incorrect ')
          return redirect(url_for('homePage'))
        
     
         

    
    
    
########################################
@app.route('/adminlogin', methods = ['GET', 'POST'])################
def adminlogin():
     return render_template('adminPage.html') 
    # if request.method == 'POST':
       # UserName = request.form.get("username")
       # Password = request.form.get("password")
       
       # adminlist= pd.read_sql_query("SELECT User_Name, User_Password FROM[dbo].[User] WHERE User_Name='"+UserName+"' AND User_Password ='"+Password+"'",con)
       # adminlist=adminlist.values
       # # sql_select_Query="SELECT User_Name, User_Password FROM[dbo].[User] WHERE User_Name='"+UserName+"'= AND User_Password ='"+Password+"';"
       # # rows=db.execute(sql_select_Query)
       # # cur = con.cursor()
       # # rows=cur.execute(sql_select_Query)
       # # rows= rows.fetchall()
       
       # if (len(adminlist) == 1):
          # session['logged_in']= True
          # return render_template('adminPage.html') 
       # else:
          # flash('Please register ')
          # return render_template('userPage.html')
          

@app.route('/adminPage', methods = ['GET', 'POST'])    
def home(): 
    clientlist=pd.read_sql_query("SELECT Client_ID, Client_name FROM[dbo].[Client]",con)
    userlist=pd.read_sql_query("SELECT User_ID, User_Email FROM[dbo].[User]",con)
    userlist=userlist.values
    clientlist=clientlist.values   
    return render_template('adminPage.html', clientlist=clientlist , userlist=userlist ) 
 

@app.route('/userPage', methods = ['GET', 'POST'])    
def homeAddUser(): 
    clientlist=pd.read_sql_query("SELECT Client_ID, Client_name FROM[dbo].[Client]",con)
    userlist=pd.read_sql_query("SELECT User_ID, User_Email FROM[dbo].[User]",con)
    userlist=userlist.values
    clientlist=clientlist.values   
    return render_template('userPage.html', clientlist=clientlist , userlist=userlist )

@app.route('/addClient', methods = ['GET', 'POST'])
def addClient():
    if request.method == 'POST':
      clientName = request.form.get("fname")
      clientType = request.form.get("type")

      con.execute("INSERT INTO [dbo].[Client](Client_name,Client_Type) VALUES ('"+clientName+"','"+clientType+"');")
      
      flash('The client added successfully')
      return redirect(url_for('home'))

@app.route('/addUser', methods = ['GET', 'POST'])
def addUser():
    if request.method == 'POST':
      AddUser = request.form.get("fname")
      AddEmail = request.form.get("Email")
      Addpasword = request.form.get("pasword")

      userlist1=pd.read_sql_query("SELECT User_ID, User_Email FROM[dbo].[User] WHERE User_Email ='"+AddEmail+"'",con)
      userlist1=userlist1.values

      if (len(userlist1) == 0):
       con.execute("INSERT INTO [dbo].[User](User_Name,User_Email,User_Password) VALUES ('"+AddUser+"','"+AddEmail+"','"+Addpasword+"');")
       flash('The User added successfully')
      else: 
       flash('The Email already exists')
      return redirect(url_for('homeAddUser'))

@app.route('/delUser', methods = ['GET', 'POST'])
def delUser():
    if request.method == 'POST':
      userEmail = request.form.get("userid")
      print(userEmail)
      try:
        con.execute("DELETE FROM [dbo].[User] WHERE User_ID ='"+userEmail+"';")
        flash('The user deleted successfully')
      except exc.SQLAlchemyError:
        flash('The user cannot be deleted.')
      return redirect(url_for('home')) 

@app.route('/delClient', methods = ['GET', 'POST'])
def delClient():
    if request.method == 'POST':
       ClientEmail = request.form.get("clientid")
       print(ClientEmail)
       try:
        con.execute("DELETE FROM [dbo].[Client] WHERE Client_ID ='"+ClientEmail+"';")
        flash('The client deleted successfully.')
       except exc.SQLAlchemyError:
        flash('The client cannot be deleted.')
       return redirect(url_for('home'))

 
        
@app.route('/logout')################
def logout():
    session.clear()
    flash('You were just logged out!')
    return(redirect(url_for('homePage')))   ################ 
    

      

############################


@app.route("/mainpage", methods = ['GET', 'POST'])
def mainpage():
  #clientlist=pd.read_sql_query("SELECT Client_ID, Client_name FROM[dbo].[Client]",con)
  clientlist=con.execute("SELECT Client_ID, Client_name FROM[dbo].[Client]")
  
  global uploadFile
  global swready
  global swreadyMessage
  uploadFile = False
  if swready == True:
    flash(swreadyMessage)
    swready = False
  return render_template("mainpage.html",clientlist=clientlist )       

@app.route('/auto', methods = ['GET', 'POST'])    
def auto(): 
    #spectroscopy command
    global Client_Name
    client_ID= request.form.get("clientid")
    Client_Name=pd.read_sql_query("SELECT Client_name FROM[dbo].[Client] where Client_ID = '"+str(client_ID)+"'",con)
    #print(Client_Name['Client_name'].iloc[0])
    client.publish("client/"+Client_Name['Client_name'].iloc[0], payload="s", qos=0)
    #client_ID = 18
    global strMessage
    #userlist=pd.read_sql_query("SELECT Measuerment_txt FROM[dbo].[Data] where Client_ID ='"+str(client_ID)+"'",con)
    #print("userlist.values")
    #print(userlist.values)
    #for txt in userlist.values:
     #   print(type(txt))
     #   filePath = os.path.join(app.config['UPLOAD_FOLDER'], 'read1.txt')
      #  f = open(filePath, 'w')
    time.sleep(15) 
        #txt.tofile(filePath, sep='\n') # send stream to predict function
      #  np.savetxt(filePath, txt, fmt="%s", newline='\r\n')
        #ef = pd.read_csv(txt.tofile(filePath, sep='\t'), sep='\t')
       # stream = np.loadtxt(filePath, dtype = np.unicode_ , delimiter="\t")
    #    print(stream)
    flash(strMessage)
    #return printFigure()
    return redirect(url_for('auto2')) 
    #return userlist.to_html() 
  
@app.route('/auto2', methods = ['GET', 'POST'])    
def auto2(): 
    #spectroscopy command
    global uploadFile
    url_plot = ""
    if uploadFile == True:
        url_plot= plot_png('read.txt')
        uploadFile = False
        return render_template('plot.html', image = url_plot )
    return render_template('plot.html', image = url_plot )
    
    
@app.route('/manual', methods = ['GET', 'POST'])
def manual():
    return render_template('prediction.html')


@app.route('/predict', methods = ['GET', 'POST'])
def predict():
    
    f = request.files.get('readingSample')
    f.seek(0)
    stream = io.StringIO(f.read().decode("UTF8"), newline=None) # send stream to predict function
    ef = pd.read_csv(stream, sep='\t')
    flash("Sample of Milk is: " + model.predictSample(ef))
    filePath = os.path.join(app.config['UPLOAD_FOLDER'], 'read11.txt')
    fi = open(filePath, 'w')
    #txt.tofile(filePath, sep='\n') # send stream to predict function
    fi.write(stream.getvalue())
    fi.close()
    url_plot= plot_png('read11.txt')
    return render_template('prediction.html', image = url_plot )
    #return redirect(url_for('predict'))


    ##---------------### 
@app.route('/background', methods = ['GET', 'POST'])    
def background(): 
    global Client_Name
    client_ID= request.form.get("clientid")
    print(client_ID)
    Client_Name=pd.read_sql_query("SELECT Client_name FROM[dbo].[Client] where Client_ID = '"+str(client_ID)+"'",con)
    print(Client_Name['Client_name'].iloc[0])
    #background command
    client.publish("client/"+str(Client_Name['Client_name'].iloc[0]), payload="i", qos=2)
    
    print("client/"+str(Client_Name['Client_name'].iloc[0]))
    #flash("The background has been activated")
    return  redirect(url_for('mainpage'))

@app.route('/reset', methods = ['GET', 'POST'])    
def reset(): 
    global Client_Name
    client_ID= request.form.get("clientid")
    Client_Name=pd.read_sql_query("SELECT Client_name FROM[dbo].[Client] where Client_ID = '"+str(client_ID)+"'",con)
    
    client.publish("client/"+Client_Name['Client_name'].iloc[0], payload="c", qos=0)
    client.unsubscribe("client/#")
    client.loop_stop()
    flash("The reset has been activated")
    return  redirect(url_for('mainpage'))

@app.route('/warmup', methods = ['GET', 'POST'])
def warmup():
    global Client_Name
    client_ID= request.form.get("clientid")
    Client_Name=pd.read_sql_query("SELECT Client_name FROM[dbo].[Client] where Client_ID = '"+str(client_ID)+"'",con)
    
    client.publish("client/"+Client_Name['Client_name'].iloc[0], payload="w", qos=0)
    flash("The warmup has been activated")
    return  redirect(url_for('mainpage'))

def connectionMqtt():
        # using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
# userdata is user defined data of any type, updated by user_data_set()
# client_id is the given name of the client


# setting callbacks, use separate functions like above for better visibility
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_publish = on_publish
    
# subscribe to all topics of encyclopedia by using the wildcard "#"
    client.subscribe("client/#", qos=2)

# a single publish, this can also be done in loops, etc.
#client.publish("client/id1", payload="client id1 hello", qos=1)

# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
    client.loop_start()
   
        
        
        