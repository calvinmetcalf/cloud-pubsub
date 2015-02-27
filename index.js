'use strict';
var Promise = require('bluebird');
var fs = require('fs');
var auth = Promise.promisify(require('google-auth2-service-account').auth);
var https = require('https');
var scope = 'https://www.googleapis.com/auth/pubsub';
var LRU = require('lru-cache');
var topicUrl = 'https://www.googleapis.com/pubsub/v1beta1/topics';
var publishUrl = 'https://www.googleapis.com/pubsub/v1beta1/topics/publish';
var subPullUrl = 'https://www.googleapis.com/pubsub/v1beta1/subscriptions/pull';
var subCreateUrl = 'https://www.googleapis.com/pubsub/v1beta1/subscriptions';
var ackUrl = 'https://www.googleapis.com/pubsub/v1beta1/subscriptions/acknowledge';
var delUrl = 'https://www.googleapis.com/pubsub/v1beta1/subscriptions/';
var delTopicUrl = 'https://www.googleapis.com/pubsub/v1beta1/topics/';
var url = require('url');
var inherits = require('inherits');
var EE = require('events').EventEmitter;

inherits(PubSub, EE);
module.exports = PubSub;
function PubSub(config){
  EE.call(this);
  var self = this;
  this.project = config.project;
  this.key = config.key;
  this.email = config.email;
  this.cache = new LRU({
    max: 500,
    maxAge: 1800 * 1000
  });
  this.topic = config.topic || 'pubsub'; 
  this.subscriptions = {};
  this.topics = {};
  this.listeners = 0;
  this.internal = new EE();
  this._emit = this.emit;
  this.emit = this.fire;
  this.on('removeListener', function () {
    self.listeners--;
    if (self.listeners === 0) {
      return self.unsubscribe();
    }

  });
  this.on('newListener', function () {
    self.listeners++;
    if (self.listeners === 1) {
      self.subscribable = self.subscribe();
    }
  });
  this.ready = this.maybeCreateTopic(this.topic);
  this.subscribable = Promise.resolve();
}

PubSub.prototype.auth = function () {
  var self = this;
  return new Promise(function(resolve, reject){
    if (self.cache.has(self.email)) {
      return resolve(self.cache.get(self.email));
    }
    auth(self.key, {
      iss: self.email,
      scope: scope
    }, function (err, resp) {
      if (err) {
        return reject(err);
      }
      self.cache.set(self.email, resp);
      resolve(resp);
    });
  });
};

PubSub.prototype.post = function (target, str, noParse, method) {
  var self = this;
  var sub;
  if (typeof noParse === 'string') {
    sub = noParse;
    noParse = false;
  }
  var aborting = false;
  return self.auth().then(function (token){
    var opts = url.parse(target);
    opts.headers = {
      Authorization: 'Bearer ' + token,
      'content-type': 'application/json'
    };
    opts.method = method || 'post';
    return new Promise(function (fullfill, reject) {
      
      var req = https.request(opts, function (resp) {
        
          // console.log(opts);
          // console.log(str);
          //console.log('aborting', aborting);
          //return reject(resp.statusCode);
        //}
        var data = [new Buffer('')];
        resp.on('error', function (e){
          self.internal.removeListener('abort', abortReq);
          if (aborting) {
            fullfill();
          } else {
            reject(e);
          }
        }).on('data', function (d){
          data.push(d);
        }).on('end', function () {
          self.internal.removeListener('abort', abortReq);
          try {
            var result = noParse?true:JSON.parse(Buffer.concat(data).toString());
          } catch (e){
            // console.log(data);
            // console.log(opts);
            reject(e);
          }
          if (resp.statusCode > 299) {
            if (resp.statusCode === 404) {
              reject(404);
            }
            //console.log(result);
            reject(result);
          } else {
            fullfill(result);
          }
        });
      }).on('error', function (e){
        self.internal.removeListener('abort', abortReq);
        if (aborting) {
          fullfill();
        } else {
          reject(e);
        }
      });
      function abortReq(name){
        aborting = true;
        //console.log('abort');
        req.abort();
      }
      self.internal.on('abort', abortReq);
      req.end(str);
    });
  });
};
PubSub.prototype.maybeCreateTopic = function (name) {
  var self = this;
  if (this.topics[name]) {
    return Promise.resolve(true);
  }
  return this.post(delTopicUrl + this.project + '/' + name, '', true, 'get').catch(function (error) {
    if (error === 404) {
      return self.createTopic(name);
    }
    throw error;
  }).then(function () {
    self.topics[name] = true;
  });
};
PubSub.prototype.createTopic = function (name) {
  return this.post(topicUrl, JSON.stringify({
    name: this.project + '/' + name
  }));
};
PubSub.prototype.removeTopic = function (name) {
  this.topics[name] = false;
  return this.post(delTopicUrl + '/topics/' + this.project + '/' + name, void 0, true, 'delete');
};

PubSub.prototype.fire = function (name, data) {
  if (name === 'newListener' || name === 'removeListener') {
    return this._emit(name, data);
  }
  // console.log('fire', name, data);
  var self = this;
  try {
    data = (new Buffer(JSON.stringify({
            name: name,
            data: data
          }))).toString('base64');
  } catch(e) {
    return Promise.reject(e);
  }
  this.ready.then(function () {
    return self.subscribable;
  }).then(function (){
    return self.post(publishUrl, JSON.stringify({
        topic: self.project + '/' + self.topic,
        message: {
          data: data
        }
    }), true);
  });
};
PubSub.prototype.subscribe = function () {
  var self = this;
  // console.log('subscribing', name);
  var name = this.topic;
  var subscriptions = this.subscriptions;
  return this.ready.then(function () {
    // console.log('ready to subscribe');
    return self.post(subCreateUrl, JSON.stringify({
      topic: self.project + '/' + name
    }));
  }).then(function (resp) {
    subscriptions[name] = resp.name;
    self.subscribable = Promise.resolve();
    self.poll(name);
  });
};
PubSub.prototype.unsubscribe = function () {
  var name = this.topic;
  var sub = this.subscriptions[name];
  this.internal.emit('abort', sub);
  if (!sub) {
    return Promise.resolve();
  }
  delete this.subscriptions[name];
  return this.post(delUrl + sub, void 0, true, 'delete');
};
PubSub.prototype.poll = function (name, subsequent, attempt) {
    var self = this;
    var sub = this.subscriptions[name];
    if (!sub) {
      if (subsequent) {
        return Promise.resolve('done');
      } else {
        return Promise.reject(new TypeError('no such subscription'))
      }
    }
    attempt = attempt || 0;
    var data = JSON.stringify({
      subscription: sub
    });
    return this.subscribable.then(function (){
      return self.post(subPullUrl, data, sub);
    }).then(function (resp) {
      if (!resp) {
        return;
      }
      var ackId = resp.ackId;
      var msg = resp.pubsubEvent.message.data;
      msg = new Buffer(msg, 'base64');
      msg = JSON.parse(msg.toString());
      return self.post(ackUrl, JSON.stringify({
        ackId: [
          ackId
        ],
        subscription: sub
      }), true).then(function (){
        self._emit(msg.name, msg.data);
        return self.poll(name, true);
      });
    }, function (e) {
      var newAttempt = attempt + 1;
      if (attempt > 10) {
        throw e;
      }
      return sleep(50 << newAttempt).then(function () {
        return self.poll(name, true, newAttempt);
      });
    });
};
function sleep (number) {
  return new Promise(function (fullfill) {
    setTimeout(fullfill, number);
  });
}
