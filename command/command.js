/**
 This file provides the interfaces for handling received commands or for reporting on what commands
 have been received.
 */

"use strict";
var dataStream          = require('./data-stream');
var globalConfig        = require('./config');
var MongoClient         = require("mongodb").MongoClient;
var Promise             = require('bluebird');
var request             = require('request');
var RequestError        = require('./request-error');
var server              = require('./server');
var stream              = require('stream');

var database = MongoClient.connect(globalConfig.mongoConnect);
var store = {};

/**
 * Add a command to the database for a specific path with the specified data.
 * @param {string} path The command path.
 * @param {object} data The data to save.
 * @returns {object} with code, content type, and content as a stream
 * { code: number, contentType: string, content: stream }
 */
exports.add = function(path, data) {
    return new Promise(function(resolve, reject) {
        var indexes;
        var item;
        var transform;
        var result = {
            code: 0,
            contentType: 'text/plain',
            content: new stream.Readable()
        };

        //set the stream to utf8 encoding
        result.content.setEncoding('utf8');

        //there is very little data being sent back with an add,
        //so this function exists to apply the DRY principle.
        function respond(code, message) {
            result.code = code;
            result.content.push(message);
            result.content.push(null);
            resolve(result);
        }

        try {
            if (store.hasOwnProperty(path)) {
                indexes = store[path].indexes;
                transform = store[path].transform;

                //run the transform function on the data
                if (transform) data = transform(data);

                //create the object to store into the database
                item = {};
                store[path].keys.forEach(function (key) {
                    var properties = indexes[key];
                    var value = data.hasOwnProperty(key) ? data[key] : properties.defaultValue;

                    //validate data
                    if (properties.required && !data.hasOwnProperty(key)) throw new RequestError('Missing required property: ' + key + '.');
                    if (properties.type && typeof value !== properties.type) throw new RequestError('Invalid type for property: ' + key + '. Expected ' + properties.type);
                    if (properties.validator && !properties.validator(value)) throw new RequestError('Validation failed for property: ' + key + '.');

                    //transform value
                    if (properties.value) value = properties.value(value);

                    //store the property value
                    item[key] = value;
                });

                //add a timestamp
                item.__timestamp = Date.now();

                //store the data to the __ property
                item.__data = data;

                //save the data to the database
                store[path].collection
                    .then(function(collection) {
                        collection.insertOne(item);
                    })
                    .then(function() {
                        respond(200, '');
                    })
                    .catch(function(err) {
                        console.error(err.stack);
                        respond(500, 'Internal server error');
                    });
            } else {
                respond(404, 'Not found');
            }
        } catch (e) {
            respond(500, e instanceof RequestError ? e.message : 'Internal server error');
        }
    });
};

/**
 * Set up a command handler to for a specified URL
 * @param {string} path The path to match before executing this command.
 * @param {object} [indexes] An object that defines what properties to
 * extract from the body to use as indexes for the data. All indexes must
 * be of the type "boolean", "number", or "string". The following
 * lists available configuration options:
 *
 *      defaultValue [optional]
 *          A default value to use if the property does not exist in the body data. This value will be applied
 *          before validation's are run so make sure it's a valid value.
 *
 *      required [optional]
 *          Set to true to require this property exists in the body data before the data can be saved.
 *          If a field is not required then you may want to specify a defaultValue, otherwise it will be null.
 *
 *      type [optional]
 *          The data type to expect. Allowed values include 'boolean', 'number', and 'string'. This property
 *          acts as a validator for type only and will validate the body's property value.
 *
 *      validator [optional]
 *          A function to call to validate the property value. This function receives the property value and
 *          should return true if valid.
 *
 *      value [optional]
 *          A function that's return result will be used as the key's value. The function receives the body's
 *          property value (after running through the validator). A common use of this option would be to return
 *          the current time stamp.
 *
 * @param {function} [transform] A function to call when data is received that does a pre-transform before attempting
 * to save the data. The returned value will represent the transformed data.
 */
exports.handle = function(path, indexes, transform) {
    var keys = Object.keys(indexes);
    if (arguments.length === 1) indexes = {};

    //validate configuration
    if (typeof indexes !== 'object') throw new Error('Command handler index must be an object');
    keys.forEach(function(key) {
        var item = indexes[key];
        if (item.type && (item.type !== 'string' && item.type !== 'number' && item.type !== 'boolean')) {
            throw new Error('Invalid type specified for command handler for ' + path + ' ' + key +
                '. Expected on of "boolean", "number", or "string".');
        }
        if (item.validator && typeof item.validator !== 'function') {
            throw new Error('Invalid validator specified for command handler for ' + path + ' ' + key +
                '. Expected a function.');
        }
        if (item.value && typeof item.value !== 'function') {
            throw new Error('Invalid value specified for command handler for ' + path + ' ' + key +
                '. Expected a function.');
        }
    });

    //validate transform
    if (transform && typeof transform !== 'function') throw new Error('Commander handler transform must be a function');

    //store the handler details
    store[path] = {
        collection: database.then(function(db) {
            return db.collection(path);
        }),
        indexes: indexes,                   //the index configuration instructions
        keys: keys,                         //a list of configuration keys
        subscriptions: {},                  //a map of subscriptions
        transform: transform                //the transform object function
    };

    //set up indexes for this collection
    keys.forEach(function(key) {
        store[path].collection.then(function(collection) {
            var index = {};
            index[key] = 1;
            collection.createIndex(key + 'Index', index);
        });
    });
};

/**
 * Make a query on a command URL.
 * @param {string} path The command path.
 * @param {object} filter The filter conditions to check against the indexes. Filters will
 * not operate against non-indexes.
 * @param {object} options The options to impose on the query:
 *
 *      endTime (default: Date.now())
 *          A number or parseable date string that will be converted to a Date and used
 *          to limit results to those that end at or before the specified time.
 *
 *      limit (default: whatever is defined in the config.js file)
 *          The number of records to return up to.
 *
 *      position (default: 1)
 *          The number of records to skip for the resultant query before returning results.
 *
 *      startTime (default: 0)
 *          A number or parseable date string that will be converted to a Date and used
 *          to limit results to those that start at or after the specified time.
 *
 *      timestampProperty (default: null)
 *          The name of the property to store the write to database timestamp on.
 */
exports.query = function(path, filter, options) {
    return new Promise(function(resolve, reject) {
        var result = {
            code: 0,
            contentType: 'text/plain',
            content: new stream.Readable(),
            limit: globalConfig.defaultLimit,
            position: null,
            recordsCount: 0
        };

        function respond(code, message) {
            result.code = code;
            if (message) result.content.push(message);
            result.content.push(null);
            resolve(result);
        }

        try {
            if (store.hasOwnProperty(path)) {
                store[path].collection
                    .then(function(collection) {
                        var query = buildQuery(path, filter, options);
                        var cursor = collection.find(query);
                        return cursor.count()
                            .then(function(count) {
                                return {
                                    count: count,
                                    cursor: cursor
                                };
                            });
                    })
                    .then(function(data) {
                        var cursor;
                        var start;
                        var tsProperty;

                        //store the records count for this query - the total number of records that match the query before limiting
                        result.recordsCount = data.count;

                        //determine the skip value
                        start = data.count - result.limit;
                        if (options.hasOwnProperty('position') && !isNaN(options.position)) start = parseInt(options.position);
                        result.position = start;

                        //if the current position falls out of bounds then return an empty response
                        if (result.position < 1 || result.position > data.count) {
                            result.contentType = 'application/json';
                            respond(200);
                            return;
                        }

                        //determine whether to return the timestamp
                        if (options.hasOwnProperty('timestamp')) {
                            tsProperty = 'timestamp';
                            if (options.timestamp) tsProperty = options.timestamp;
                        }

                        //limit the cursor
                        cursor = data.cursor.skip(start - 1);

                        //set status and content type
                        result.code = 200;
                        result.contentType = 'application/json';

                        //set the content
                        result.content = cursor.stream({
                            transform: function(item) {
                                var data = item.__data;
                                if (tsProperty) data[tsProperty] = item.__timestamp;
                                return data;
                            }
                        });

                        //resolve the result
                        resolve(result);
                    })
                    .catch(function(e) {
                        console.error(e.stack);
                        respond(500, 'Internal server error');
                    });

            } else {
                respond(404, 'Not found');
            }
        } catch (e) {
            console.error(e.stack);
            respond(500, 'Internal server error');
        }
    });
};




exports.query2 = function(path, filter, options) {
    return new Promise(function(resolve, reject) {
        var result = {
            code: 0,
            contentType: 'text/plain',
            content: null,
            limit: globalConfig.defaultLimit,
            position: null,
            recordsCount: 0
        };

        function respond(code, message) {
            result.code = code;
            result.content = new stream.Readable();
            result.content.setEncoding('utf8');
            if (message) result.content.push(message);
            result.content.push(null);
            resolve(result);
        }

        try {
            if (store.hasOwnProperty(path)) {
                store[path].collection
                    .then(function(collection) {
                        var query = buildQuery(path, filter, options);
                        var cursor = collection.find(query);
                        return cursor.count()
                            .then(function(count) {
                                return {
                                    count: count,
                                    cursor: cursor
                                };
                            });
                    })
                    .then(function(data) {
                        var cursor;
                        var start;
                        var tsProperty;

                        //store the records count for this query - the total number of records that match the query before limiting
                        result.recordsCount = data.count;

                        //limit the number of results
                        if (options.hasOwnProperty('limit') && !isNaN(options.limit)) result.limit = parseInt(options.limit);
                        if (result.limit > globalConfig.maxLimit) result.limit = globalConfig.maxLimit;

                        //determine the skip value
                        start = data.count - result.limit;
                        if (options.hasOwnProperty('position') && !isNaN(options.position)) start = parseInt(options.position);
                        result.position = start;

                        //if the current position falls out of bounds then return an empty response
                        if (result.position < 1 || result.position > data.count) {
                            result.contentType = 'application/json';
                            respond(200);
                            return;
                        }

                        //determine whether to return the timestamp
                        if (options.hasOwnProperty('timestamp')) {
                            tsProperty = 'timestamp';
                            if (options.timestamp) tsProperty = options.timestamp;
                        }

                        //limit the cursor
                        cursor = data.cursor
                            .skip(start - 1)
                            .limit(result.limit);

                        //set status and content type
                        result.code = 200;
                        result.contentType = 'application/json';

                        //set the content
                        result.content = cursor.stream({
                            transform: function(item) {
                                var data = item.__data;
                                if (tsProperty) data[tsProperty] = item.__timestamp;
                                return data;
                            }
                        });

                        //resolve the result
                        resolve(result);
                    })
                    .catch(function(e) {
                        console.error(e.stack);
                        respond(500, 'Internal server error');
                    });

            } else {
                respond(404, 'Not found');
            }
        } catch (e) {
            console.error(e.stack);
            respond(500, 'Internal server error');
        }
    });
};

exports.subscribe = function(path, query, listener) {
    return new Promise(function(resolve, reject) {
        var result = {
            code: 0,
            contentType: 'text/plain',
            content: new stream.Readable()
        };

        function respond(code, message) {
            result.code = code;
            result.content.push(message);
            result.content.push(null);
            resolve(result);
        }

        try {
            if (store.hasOwnProperty(path)) {
                //TODO: figure out how to generate a unique key for the subscription
                //the unique key must be used to unsubscribe
            } else {
                respond(404, 'Not found');
            }
        } catch (e) {
            console.error(e.stack);
            respond(500, 'Internal server error');
        }
    });
};

exports.unsubscribe = function(path, query, listener) {
    return new Promise(function(resolve, reject) {
        var result = {
            code: 0,
            contentType: 'text/plain',
            content: new stream.Readable()
        };

        function respond(code, message) {
            result.code = code;
            result.content.push(message);
            result.content.push(null);
            resolve(result);
        }

        try {
            if (store.hasOwnProperty(path)) {

            } else {
                respond(404, 'Not found');
            }
        } catch (e) {
            console.error(e.stack);
            respond(500, 'Internal server error');
        }
    });
};

function buildQuery(path, filter, options) {
    var endTime;
    var item = store[path];
    var result = {};
    var startTime;

    Object.keys(filter).forEach(function(key) {
        var value;
        var type;
        if (item.indexes.hasOwnProperty(key)) {
            type = item.indexes[key].type;
            value = filter[key];
            switch(type) {
                case 'boolean':
                    if (value === 'true') value = true;
                    if (value === 'false') value = false;
                    result[key] = !!value;
                    break;
                case 'number':
                    if (!isNaN(value)) {
                        result[key] = /\./.test(value) ? parseFloat(value) : parseInt(value);
                    }
                    break;
                case 'string':
                default:
                    result[key] = value;
                    break;
            }
        }
    });

    if (options.hasOwnProperty('startTime')) startTime = parseTime(options.startTime);
    if (options.hasOwnProperty('endTime')) startTime = parseTime(options.endTime);
    if (typeof startTime !== 'undefined' && typeof endTime !== 'undefined') {
        result.__timestamp = { $gte: startTime, $lte: endTime };
    } else if (typeof startTime !== 'undefined') {
        result.__timestamp = { $gte: startTime };
    } else if (typeof endTime !== 'undefined') {
        result.__timestamp = { $lte: endTime };
    }

    return result;
}

/**
 * Take in a parseable time and convert it into a timestamp
 * @param {string} time
 * @returns {number}
 */
function parseTime(time) {
    var t;
    if (!isNaN(time)) return parseInt(time);
    t = new Date(time);
    if (isNaN(t)) return null;
    return t.valueOf();
}
