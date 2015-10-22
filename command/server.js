"use strict";
var bodyParser          = require('body-parser');
var command             = require('./command');
var engine              = require('engine.io');
var express             = require('express');
var favicon             = require('serve-favicon');
var http                = require('http');
var subscribe           = require('./subscribe');
var through2            = require('through2');

var app = express();
var server = http.createServer(app).listen(3000);
//var socketServer = engine.attach(server);
var configurations = {};

app.use(bodyParser.json());
app.use(favicon(__dirname + '/favicon.ico'));

app.post('*', function(req, res, next) {
    var url = analyzeUrl(req.url);
    switch (url.action) {
        case 'command':
            command.add(url.path, req.body, res, next);
            break;
        default:
            next();
    }
});

app.get('*', function(req, res, next) {
    var queryParams = analyzeQueryParameters(req.query);
    var url = analyzeUrl(req.url);
    var host = req.protocol + '://' + req.get('host') + '/' + url.path;

    switch (url.action) {
        case 'query':

            command.query(url.path, queryParams.filter, queryParams.options)
                .then(function(result) {
                    var first = true;
                    res.status(result.code);
                    res.set('Content-Type', result.contentType);
                    if (result.code !== 200) {
                        result.content.pipe(res);
                    } else {
                        result.content
                            .pipe(through2.obj(
                                function (chunk, enc, callback) {
                                    if (first) {
                                        pushQueryStreamStart(this, result, host, req.query);
                                        first = false;
                                    } else {
                                        this.push(',')
                                    }
                                    this.push(JSON.stringify(chunk));
                                    callback();
                                },
                                function (callback) {
                                    if (first) pushQueryStreamStart(this, result, host, req.query);
                                    pushQueryStreamEnd(this);
                                    callback();
                                }
                            ))
                            .pipe(res);
                    }
                });
            break;
        case 'subscribe':
            subscribe.start(url.path, req, res);
            break;
        case 'unsubscribe':
            subscribe.stop(url.path, req, res);
            break;
        default:
            next();
    }
});

server.on('connection', function(socket){
    var t = Date.now();
    console.log('connection made [ ' + t + ' ]');
    //socket.send('utf 8 string');
    //socket.send(new Buffer([0, 1, 2, 3, 4, 5])); // binary data
    socket.on('close', function(){
        console.log('connection closed [ ' + t + ' : ' + Math.round((Date.now() - t) / 1000) + 's ]');
    });
});






function analyzeQueryParameters(query) {
    var result = {
        filter: {},
        options: {}
    };
    Object.keys(query).forEach(function(key) {
        if (/^~/.test(key)) {
            result.options[key.substr(1)] = query[key];
        } else {
            result.filter[key] = query[key];
        }

        /*if (key === 'filter') {
            query[key].split(',').forEach(function(kvPair) {
                var ar = kvPair.split(':');
                result.filter[ar[0]] = ar[1];
            })
        } else {
            result.options[key] = query[key];
        }*/
    });
    return result;
}

function analyzeUrl(url) {
    var urlPathArray = url.split('?')[0].replace(/^\//, '').replace(/\/$/, '').split('/');
    return {
        action: urlPathArray[0],
        path: urlPathArray.slice(1).join('/')
    };
}

function buildQueryString(query, config) {
    var data = Object.assign({}, query, config);
    var result = '?';
    Object.keys(data).forEach(function(key, index) {
        if (index > 0) result += '&';
        result += key + '=' + data[key];
    });
    return result;
}

function pushQueryStreamStart(context, result, serviceUrl, query) {
    var limit;
    var o;
    var pos;
    var serviceUrlHasParameters = serviceUrl.split('?')[1];
    var url = serviceUrl + '/';

    context.push('{');

    context.push('"links":{');

    //latest hateoas link
    context.push('"latest":{');
    context.push('"href":"' + serviceUrl + '",');
    context.push('"method":"GET",');
    context.push('"rel":"latest"');
    context.push('},');

    //previous hateoas link
    if (result.recordsCount > result.position + result.limit) {
        o = { '~position': result.position + result.limit };
        context.push('"next":{');
        context.push('"href":"' + url + buildQueryString(query, o) + '",');
        context.push('"method":"GET",');
        context.push('"rel":"next"');
        context.push('},');
    }

    //next hateoas link
    if (result.position > 1) {
        o = { '~position': result.position - result.limit };
        if (o['~position'] < 1) {
            o['~position'] = 1;
            o['~limit'] = result.position - 1;
        }
        context.push('"prev":{');
        context.push('"href":"' + url + buildQueryString(query, o) + '",');
        context.push('"method":"GET",');
        context.push('"rel":"prev"');
        context.push('},');
    }

    //self hateoas link
    context.push('"self":{');
    context.push('"href":"' + url + buildQueryString(query, { '~position': result.position }) + '",');
    context.push('"method":"GET",');
    context.push('"rel":"self"');
    context.push('}');

    context.push('},');

    context.push('"metadata":{');
    context.push('"page_size":' + result.limit + ',');
    context.push('"position":' + result.position + ',');
    context.push('"total_records":' + result.recordsCount + '');
    context.push('},');

    context.push('"values":[');
}

function pushQueryStreamEnd(context) {
    context.push(']');
    context.push('}');
    context.push(null);
}














