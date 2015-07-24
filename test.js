'use strict';
var data = require('./data.json');


var PubSub = require('./index');
var test = require('tape');
test('first', function (t) {
  t.plan(2);
  var sub = new PubSub(data);

  console.log('ready');
  sub.on('foo', f1);
  function f1(a) {
    t.ok(a, JSON.stringify(a));
    sub.emit('bar', 'bar');
    sub.removeListener('foo', f1);
  }
  sub.on('bar', f2);
  function f2(a) {
    t.ok(a, JSON.stringify(a));
    sub.removeListener('bar', f2);
  }
  sub.emit('foo', 'foo');

});
