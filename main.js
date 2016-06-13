/**
 * Created by gaozhu on 2015/4/10.
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
//var baseDir = "C:\\GkProgram";


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


try{
    var conf = JSON.parse(fs.readFileSync(path.join(baseDir,"conf.json")));
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
    var dbBasePath = path.join(baseDir,"leveldb");

    var level = require('levelup');
    var leveldown = require("leveldown");
    var db = level(dbBasePath,{
        db: leveldown,
        valueEncoding:'json'
    });

//保存消息到数据库
    var saveMsg = function(msg){
        var timeStamp = new Date(msg.time).getTime();
        db.put('msg'+timeStamp, msg, function (err) {
            err && saveMsg(msg);
        });
    }

    var offsetStream = require('offset-stream');
    var through      = require('ordered-through');
    var fix          = require('level-fix-range');

    var io = require('socket.io').listen(17777);
    io.sockets.on('connection', function (socket) {
        socket.on('alive', function (serverName) {
            socket.serverName = serverName;
        });
        socket.on('updateTime', function (updateTime,callback) {
            if(db.isClosed()){
                return;
            }
            if(!updateTime){
                db.get("updateTime",function(err,value){
                    callback && callback(value);
                });
            }else
                db.put("updateTime",updateTime);
        });
        socket.on("query",function(params,callback) {
            var datas = [];
            if(db.isClosed()){
                callback && callback({err:"数据库已关闭，无法提供查询!"});
                return;
            }
            var start = "msg" + new Date(params.startTime).getTime();
            var end = "msg" + new Date(params.endTime).getTime();
            var offset = params.offset;
            var limit = offset + params.limit;
            db.createKeyStream(fix({
                reverse : false,
                start   : start,
                end     : end,
                limit   : limit
            }))
                .pipe(offsetStream(offset))
                .pipe(through(db.get.bind(db)))
                .on("data",function(data){
                    datas.push(data);
                })
                .on("error",function(err){
                    callback && callback({err:"查询出错啦！"});
                })
                .on('end', function () {
                    callback && callback({offset:offset,datas:datas})
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
            try{
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

                var count = msg.points.length;
                for(var i in msg.points){
                    if(!msg.points[i].name || msg.points[i].name.indexOf(conf.ucode) != 0){
                        delete msg.points[i];
                        count --;
                    }
                }
                if(count > 0){
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
    var startCmd = "call \"" + path.join(baseDir,"nw.exe") +"\"  \"" + path.join(baseDir,"gkserver.nw") + "\"";
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

}catch(e){
    log.error(e);
}

