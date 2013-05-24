var util = require('util');
var CloudWatch = require("awssum-amazon-cloudwatch").CloudWatch;
var fmt = require('fmt');
var getIamCreds = require('./get-iam-creds');

function CloudwatchBackend(startupTime, config, emitter) {
    var self = this;
    var awsConf = {
    }; 
  
    // if there is a config that was provided use it ... otherwise attempt to 
    // fetch it from IAM using the meta-data
    
    //config.cloudwatch.region = config.cloudwatch.region ? amazon[config.cloudwatch.region] : null;


    /* This updates `awsConf` with information from the IAM Role, EC2 meta data 
     * It also sets a callback to update itself 
     *
     **/
    var timeout = 0;
    function updateCredsFromIAM() {
        console.log("Fetching IAM Credentials");
        getIamCreds(function(err, creds) {
            if (err) {
                console.error("ERROR: ", err);
                return; 
            } else {
                console.log("Got Creds", creds);
            }

            awsConf.accessKeyId = creds.accessKeyId;
            awsConf.secretAccessKey = creds.secretAccessKey;
            awsConf.token = creds.token;

            // auto-update the credentials when they expire
            var max_time = 5 * 60 * 1000; 
            if (creds.expires >= max_time) {
                console.log("Refreshing IAM Credentials in " + max_time + "ms");
                timeout = setTimeout(updateCredsFromIAM, max_time)
            } else {
                console.log("Refreshing IAM Credentials in " + creds.expires + "ms");
                timeout = setTimeout(updateCredsFromIAM, creds.expires)
            }
        });
    };

    if (config.cloudwatch && config.cloudwatch.useIAM == true ) {
        updateIAMCreds();
    } else {
        // fetch the credentials...
        awsConf = config.cloudwatch; 
    }

    // attach
    emitter.on('flush', function(timestamp, metrics) { 
        if (awsConf == false) {
            console.error("NO CW AWS conf")
            return; 
        }

        console.log("FLUSH", timestamp, metrics);
        //self.flush(awsConf, timestamp, metrics); 
    });
};

CloudwatchBackend.prototype.processKey = function(key, sep_re) {
    if (sep_re.constructor === Boolean)
        sep_re = "[\.\/-]";
    var parts = key.split(new RegExp(sep_re));
    var metricName = parts[parts.length-1]; 
    if (parts.slice(0,parts.length-1).length == 0) {
        var namespaceParts = null;
        var dimensionsParts = null; 
    } else {
        var namespaceParts = parts.slice(0,parts.length-1).filter(function(p){ return /^__/.exec(p) == null; });
        // dimension naming parts start with "__", key and value are separated by "_"
        var dimensionParts = parts.slice(0,parts.length-1).filter(function(p){ return /^__/.exec(p) != null; });
        dimensionParts = dimensionParts.map(function(p){ return p.slice(2); });
    }
    return {
        metricName: metricName,
        namespace: namespaceParts ? namespaceParts.join("/") : null,
        dimensionMap: dimensionParts ? dimensionParts.map(function(x) { pp=x.split(/_/); return { Name:pp[0], Value:pp[1] } }) : null
    };
}

CloudwatchBackend.prototype.prepareCWData = function(key, data) {

    var names = this.config.processKeyForNamespace ? this.processKey(key, this.config.processKeyForNamespace) : {};
    var namespace = this.config.namespace || names.namespace || "AwsCloudWatchStatsdBackend";
    var metricName = this.config.metricName || names.metricName || key;

    var dimensionMap =  this.config.dimensions || null;
    if (names.dimensionMap != null) {
        dimensionMap = names.dimensionMap.concat(dimensionMap != null ? dimensionMap : []);
    }

    data.Namespace = namespace;
    data.MetricData.forEach(function(md) {
        md.MetricName = metricName;
        if (dimensionMap != null) {
            md.Dimensions = dimensionMap;
        }
    });
    
    return data;

}

CloudwatchBackend.prototype._addMetricData = function(packets, data) {

    if (!(data.Namespace in packets)) {
        packets[data.Namespace] = [{ "Namespace": data.Namespace, "MetricData": [] }];
    }
    last_packet = packets[data.Namespace].slice(-1)[0];
    for (var i = 0; i < data.MetricData.length; i++) {
        /* Keep packets from getting too big, awssum is not doing any smart handling of limits */
        if (last_packet["MetricData"].length >= 8) {
            last_packet = { "Namespace": data.Namespace, "MetricData": [] };
            packets[data.Namespace].push(last_packet);
        }
        last_packet.MetricData.push(data.MetricData[i]);
    }
    
}

CloudwatchBackend.prototype.flush = function(conf, timestamp, metrics) {

    var cloudwatch = new CloudWatch(conf);

    var counters = metrics.counters;
    var gauges = metrics.gauges;
    var timers = metrics.timers;
    var sets = metrics.sets;
    
    var cwPackets = {}
    
    for (key in counters) {
        if (key.indexOf('statsd.') == 0)
            continue;
        md = this.prepareCWData(key, {
            MetricData : [{
                Unit : 'Count',
                Timestamp: new Date(timestamp*1000).toISOString(),
                Value : counters[key]
            }]
        });
        this._addMetricData(cwPackets, md);
    }

    for (key in timers) {
        if (timers[key].length > 0) {
            var values = timers[key].sort(function (a,b) { return a-b; });
            var count = values.length;
            var min = values[0];
            var max = values[count - 1];

            var cumulativeValues = [min];
            for (var i = 1; i < count; i++) {
                cumulativeValues.push(values[i] + cumulativeValues[i-1]);
            }

            var sum = min;
            var mean = min;
            var maxAtThreshold = max;

            var message = "";

            var key2;

            sum = cumulativeValues[count-1];
            mean = sum / count;
            
            md = this.prepareCWData(key, {
                MetricData : [{
                    Unit : 'Milliseconds',
                    Timestamp: new Date(timestamp*1000).toISOString(),
                    StatisticValues: {
                        Minimum: min,
                        Maximum: max,
                        Sum: sum,
                        SampleCount: count
                    }
                }]
            });
            this._addMetricData(cwPackets, md);
        }
    }

    for (key in gauges) {
        md = this.prepareCWData(key, {
            MetricData : [{
                Unit : 'None',
                Timestamp: new Date(timestamp*1000).toISOString(),
                Value : gauges[key]
            }]
        });
        this._addMetricData(cwPackets, md);
    }

    for (key in sets) {
        md = this.prepareCWData(key, {
            MetricData : [{
                Unit : 'None',
                Timestamp: new Date(timestamp*1000).toISOString(),
                Value : sets[key].values().length
            }]
        });
        this._addMetricData(cwPackets, md);        
        statString += 'stats.sets.' + key + '.count ' + sets[key].values().length + ' ' + ts + "\n";
        numStats += 1;
    }

    console.log("FLUSH\n", cwPackets); 
    return;

    /* Do the actual sending of data */
    for (ns in cwPackets) {
        for (var i = 0; i < cwPackets[ns].length; i++) {
            cloudwatch.PutMetricData(
                cwPackets[ns][i],
                function(err, data) {
                    fmt.dump(err, 'Err');
                    fmt.dump(data, 'Data');
                }
            );
        }
    }
    
};

exports.init = function(startupTime, config, emitter) {
    var instance = new CloudwatchBackend(startupTime, config, emitter);
    return true;
};

