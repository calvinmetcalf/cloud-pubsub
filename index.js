var Promise = require('bluebird');
var fs = require('fs');
var auth = Promise.promisify(require('google-auth2-service-account').auth);
var https = require('https');
var scope = 'https://www.googleapis.com/auth/pubsub';

function PubSub(config){
  this.project = config.project;
  if(typeof config.key === 'string') {
    this.key = fs.readFileSync(config.key);
  } else if(Buffer.isBuffer(config.key)) {
    this.key = config.key;
  }
  this.email = config.email;
}

PubSub.prototype.createTopic = function (name) {

}