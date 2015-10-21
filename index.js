"use strict";
var command             = require('./server/command');

command.handle('v1/logs',
    {
        type: {
            required: true,
            type: 'string',
            validator: function(value) {
                return ['info', 'warn', 'error'].indexOf(value) !== -1;
            }
        }
    }
);

/*
var msgNum = 1;
setInterval(function() {
    var type;
    var random;

    random = Math.round(Math.random() * 10);
    if (random < 6) {
        type = 'info';
    } else if (random < 9) {
        type = 'warn';
    } else {
        type = 'error';
    }

    command.add('v1/logs', {
        type: type,
        message: 'Message #' + msgNum++
    });
}, 1000);*/
