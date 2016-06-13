/**
 * Created by gaozhu on 2015/3/24.
 */
Date.prototype.format = function (format) {
    var o = {
        "M+": this.getMonth() + 1,
        "d+": this.getDate(),
        "h+": this.getHours(),
        "m+": this.getMinutes(),
        "s+": this.getSeconds(),
        "q+": Math.floor((this.getMonth() + 3) / 3),
        "S": this.getMilliseconds()
    }
    if (/(y+)/.test(format)) {
        format = format.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    }
    for (var k in o) {
        if (new RegExp("(" + k + ")").test(format)) {
            format = format.replace(RegExp.$1, RegExp.$1.length == 1 ? o[k] : ("00" + o[k]).substr(("" + o[k]).length));
        }
    }
    return format;
}

var path = require('path');
var fs = require("fs");
//执行路径
var baseDir = path.dirname(process.execPath);
//var baseDir = "C:\\Users\\gaozhu\\Desktop";
//var baseDir = "C:\\Users\\gaozhu\\Desktop\\gkClient";
var conf = JSON.parse(fs.readFileSync(path.join(baseDir,"conf.json")));

var logPath = path.join(baseDir,"logs");
!fs.existsSync(logPath) && fs.mkdirSync(logPath);
var log4js = require('log4js');
log4js.configure({
    appenders: [
        { type: 'file', filename: path.join(logPath, "msg.log"), "maxLogSize": 10 * 1024 *1024 * 1024,
            pattern: "%d - %m%n", "backups": 5, category: 'msg' },
        { type: 'file', filename: path.join(logPath, "main.log"), "maxLogSize": 20 * 1024 * 1024,
            pattern: "%d - %m%n", "backups": 3, category: 'gk-auto-connector' }
    ]
});

var msgLog = log4js.getLogger('msg');
var log = log4js.getLogger('gk-auto-connector');

if(!conf.ucode){
    exitWhenErr("需要配置安装目录下的conf.json文件中的参数：ucode!")
}

//当异常的时候退出
var exitWhenErr =function(err){
    err && log.error("无法处理的异常:" + err);
    io && io.close();
    server && server.close();
    err && setTimeout(function(){
        process.exit(0);
    },2000);
}

process.on("uncaughtException", function (e) {
    if(e.code === "EADDRINUSE"){
        exitWhenErr("服务已开启，重复开启！");
    }
    log.error(e);
    return;
});
var dbBasePath = path.join(baseDir,"TingoDB");
!fs.existsSync(dbBasePath) && fs.mkdirSync(dbBasePath);
var Db = require('tingodb')().Db;
var dbNow;
var dbQuery;
var collNow;
var collQuery;
var Queue = require('file-queue').Queue,queue;
var queuePath = path.join(baseDir,"Queue");
!fs.existsSync(queuePath) && fs.mkdirSync(queuePath);
queue = new Queue(queuePath, function(err) {
    if(err){ exitWhenErr("队列创建失败:"+err);}
});


var getCollNow = function(timeStr){
    if(!timeStr)
        return collNow;
    if(dbNow && timeStr.indexOf(dbNow.dateStr) == 0)
        return collNow;
    dbNow && dbNow.close();
    var dateStr = new Date(timeStr).format("yyyy-MM-dd");
    var dbPath = path.join(dbBasePath,dateStr);
    !fs.existsSync(dbPath) && fs.mkdirSync(dbPath);
    dbNow = new Db(dbPath, {});
    dbNow.dateStr = dateStr;
    collNow = dbNow.collection("message"+dateStr);
    dbNow.createIndex('message'+dateStr, {time:1} , {unique:true, background:true, w:1}, function(err, indexName) {
    });
    return collNow;
};

var getCollQuery = function(timeStr){
    if(!timeStr)
        return;
    if(dbNow && timeStr.indexOf(dbNow.dateStr) == 0)
        return collNow;
    if(dbQuery && timeStr.indexOf(dbQuery.dateStr) == 0)
        return collQuery;
    collQuery && collQuery.close();
    var dateStr = new Date(timeStr).format("yyyy-MM-dd");
    var dbPath = path.join(dbBasePath,dateStr);
    if(!fs.existsSync(dbPath))
        return null;
    dbQuery = new Db(dbPath, {});
    dbQuery.dateStr = dateStr;
    collQuery = dbQuery.collection("message"+dateStr);
    dbQuery.createIndex('message'+dateStr, {time:1} , {unique:true, background:true, w:1}, function(err, indexName) {
    });
    return collQuery;
}

getCollNow(new Date().format("yyyy-MM-dd hh:mm:ss"));
//保存消息到数据库
var saveMsg = function(msg){
    getCollNow(msg.time).insert(msg,{w:1}, function (err, result) {
        err && saveMsg(msg);
    });
}

var io = require('socket.io').listen(17777);
io.sockets.on('connection', function (socket) {
    socket.on('alive', function (serverName) {
        socket.serverName = serverName;
    });
    socket.on("query",function(params,callback) {
        var coll = getCollQuery(params.startTime);
        if (!coll) {
            callback && callback({err: "no data"});
            return;
        }
        var isNextPage = false;

        var offset = params.offset + params.type * params.limit;

        if (offset == 0 && params.type == 0)
            isNextPage = false;
        else if(offset == 0)
            isNextPage = params.type > 0
        else{
            isNextPage = offset > 0;
        }

        coll && !isNextPage && coll.find({time:{$lt:params.startTime,$gt : "1990-01-01 00:00:01"}}).sort({time:-1}).skip(Math.abs(offset)).limit(params.limit).toArray(function(err,datas){
            if(!err)
                params.offset = params.offset + params.type * datas.length;
            if(params.type == 0)
                params.offset = -datas.length;
            if(params.offset > 0)
                params.offset = 0;
            callback && callback({err:err,offset:params.offset,datas:datas.reverse()});
        });

        coll && isNextPage && coll.find({time:{$gte:params.startTime,$gt : "1990-01-01 00:00:01"}}).sort({time:1}).skip(offset).limit(params.limit).toArray(function(err,datas){
            if(!err)
                params.offset = params.offset + params.type * datas.length;
            if(params.offset < 0)
                params.offset = 0;
            callback && callback({err:err,offset:params.offset,datas:datas});
        });
    });
    socket.on("stop",function(){
       exitWhenErr("客户端强行终止！")
    });
});

//开启奥特数采接收程序
var thrift = require('thrift');
var MetaqService = require('./gen-nodejs/MetaqService.js'),
    ttypes = require('./gen-nodejs/metaq_types.js');
var server = thrift.createServer(MetaqService, {
    sendMsg: function (msg, result) {
        msgLog.info(msg);
        try{
            msg = eval("["+msg+ "]")[0];
        }catch(e){
            log.error(e);
            result(null, true);
            return;
        }

        if(!msg || !msg.points || msg.points.length < 0){
            result(null, true);
            return;
        }
        try{
            var count = msg.points.length;
            for(var i in msg.points){
                if(!msg.points[i].name || msg.points[i].name.indexOf(conf.ucode) != 0){
                    delete msg.points[i];
                    count --;
                }
            }
            if(count > 0){
                queue.isRunning() && queue.push(msg,function(err){
                    err && log.error(err);
                });
                saveMsg(msg);
                io.sockets.emit("msg",msg);
            }
        }catch(e){
        }
        result(null, true);
    }
}, {
    transport: thrift.TFramedTransport,
    protocol: thrift.TBinaryProtocol
});

server.listen(17788);

var exec = require('child_process').exec;
var startCmd = "call \"" + path.join(baseDir,"gkserver.exe") +"\"";
var ioClient = require("socket.io-client");
var socket = ioClient.connect("http://localhost:17776",{
    'force new connection': true,  // 是否允许建立新的连接
    reconnect: true,           // 是否允许重连
    'reconnection delay': 1000, // 重连时间间隔 毫秒
    'max reconnection attempts': Number.MAX_VALUE // 重连次数上限
});
socket.on("connect", function(){
    socket.emit("alive","gk-auto-connector");
});
socket.on("reconnect_attempt", function (times) {
    log.info("重连次数："+times);
    if(times % 10 == 0){
        exec(startCmd,function(err){
            log.error(err);
        });
    }
})

