'''
Created on 25 Jul 2016

@author: marco
'''
from flask import Flask
from flask import request
from flask import jsonify
from flask import render_template
import os
import sys
from aws_connector import  EC2_Client


app = Flask(__name__)
ec2_client = EC2_Client()

@app.route('/ec2',methods=['GET', 'POST'])
def manage_ec2():
    json = request.json
    print 'Json requet:%s' % json
    if json is not None:
        method_name = json['method']
        method_params = json.get('params', {})
        print 'MethodName:%s|MethodParams:%s' % (method_name, method_params)
        response = ec2_client.list_instances() #getattr(ec2_client, method_name)(**method_params)
    return jsonify({'root': response})        

@app.route('/sns',methods=['GET', 'POST'])
def manage_sns():
    json = request.json
    print 'Json requet:%s' % json
    if json is not None:
        method_name = json['method']
        method_params = json.get('params', {})
        response = getattr(ec2_client, method_name)(**method_params)
    return jsonify({'root': response})        

def invalidRequest():
    return jsonify({'root':{'Error':'Invalid Request'}})


def handler():
    print 'Shutting down...'
    f = open('log.txt', 'w')
    f.write('shutdown...')
    f.close()
    sys.exit()

#signal.signal(signal.SIGTERM, handler)
#signal.pause()

if __name__== "__main__":
        app.run(host='0.0.0.0')

