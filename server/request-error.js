
module.exports = RequestError;

function RequestError(message) {
    this.name = 'RequestError';
    this.message = message || 'The request is malformed';
    this.stack = (new Error()).stack;
}
RequestError.prototype = Object.create(Error.prototype);
RequestError.prototype.constructor = RequestError;