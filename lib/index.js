
var debug    = require('debug')('S3Exporter')
  , stream   = require('readable-stream')
  , util     = require('util')
  , zlib     = require('zlib');


module.exports = S3Exporter;


/**
 * Create an S3 Exporter stream
 * @param {Object} s3  an s3 knox client
 * @param {Object} options
 *    @field {Boolean} gzip  whether the input is gzipped
 */
function S3Exporter(s3, options) {
  options || (options = {});
  stream.Transform.call(this, options);
  this._writableState.objectMode = true;

  this.s3      = s3;
  this.options = options;
}
util.inherits(S3Exporter, stream.Transform);


/**
 * Transforms S3 log file descriptions into a stream of their contents
 * @param  {Object}   file
 * @param  {String}   encoding
 * @param  {Function} callback
 */
S3Exporter.prototype._transform = function (file, encoding, callback) {

  var self   = this
    , path   = file.Key;

  debug('Reading: %s', path);

  this.s3.getFile(path, function (err, res) {
    if (err) return callback(err);

    if (self.options.gzip) res = res.pipe(zlib.createGunzip());

    res.on('data',  function (data) { self.push(data); })
       .on('error', function (err)  { callback(err);   })
       .on('end',   function ()     { callback();      });
  });
};
