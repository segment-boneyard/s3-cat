
var assert     = require('assert')
  , auth       = require('./auth.json')
  , knox       = require('knox')
  , S3Lister   = require('s3-lister')
  , S3Exporter = require('..');


var client = knox.createClient(auth);


var once = function (count, fn) {
  var called = 0;
  return function (err) {
    if (err) return fn(err);
    called += 1;
    if (called >= count) fn();
  };
};


describe('S3Exporter', function () {

  var files  = 3
    , folder = '_s3-exporter-test';

  before(function (done) {
    done = once(files, done);

    for (var i = 0; i < files; i++) {
      var filename = folder + '/' + i + '.txt';
      client.putBuffer(i.toString(), filename, done);
    }
  });


  it('should list the full file contents', function (done) {
    this.timeout(10000);

    var lister   = new S3Lister(client, { prefix : folder })
      , exporter = new S3Exporter(client)
      , output   = '';

    lister.pipe(exporter)
      .on('data', function (data) { output += data; })
      .on('end',  function () { assert(output === '012'); done(); });
  });


  after(function (done) {
    done = once(files, done);

    for (var i = 0; i < files; i++) {
      var filename = folder + '/' + i + '.txt';
      client.deleteFile(filename, done);
    }
  });
});
