"use strict";
var through2                = require('through2');

exports.httpTransform = function() {
    var first = true;
    through2.obj(
        function(chunk, enc, callback) {
            if (first) {
                this.push('{');
                this.push('"metadata":{');
                this.push('"pageNumber":' + page + ',');
                this.push('"pageSize":' + limit + ',');
                this.push('"resultCount":' + data.count + '');
                this.push('},');
                this.push('"values":[');
                first = false;
            } else {
                this.push(',')
            }
            this.push(JSON.stringify(chunk.__data));
            callback();
        },
        function(callback) {
            this.push(']');
            this.push('}');
            callback();
        }
    )
};