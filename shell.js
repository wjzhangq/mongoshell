nv = {
  helpItem:{'db':{}, 'coll':{}},
  original_help:{},
  extend:function(obj, funcName, func, funcHelp){
    obj.prototype[funcName] = func;
    if (typeof(funcHelp) == 'undefined'){
      funcHelp = funcName + "() - No help";
    }
    
    if (obj === DB){
      nv.helpItem.db[funcName] = funcHelp;
      if (obj.prototype.help !== nv.dbHelp){
        nv.dbOriginalHelp = obj.prototype.help;
        obj.prototype.help = nv.dbHelp;
      }
    }else if (obj === DBCollection){
      nv.helpItem.coll[funcName] = funcHelp;
      if (obj.prototype.help !== nv.collOriginalHelp){
        nv.collOriginalHelp = obj.prototype.help;
        obj.prototype.help = nv.collHelp;
      }
    }
  },
  dbHelp:function(){
    if (typeof(nv.dbOriginalHelp) == 'function'){
      nv.dbOriginalHelp.apply(this);
    }
    for(var i in nv.helpItem.db){
      print("\tdb." + nv.helpItem.db[i]);
    }
  },
  collHelp:function(){
    if (typeof(nv.collOriginalHelp) == 'function'){
      nv.collOriginalHelp.apply(this);
    }
    var shortName = this.getName();
    for(var i in nv.helpItem.coll){
      print("\tdb." + shortName + "." + nv.helpItem.coll[i]);
    }
  }
};

//for db.nvBalancer detail:http://wiki.zhangwenjin.com/doku.php?id=youku:mongo:shelljs#dbnvbalancer
nv.nvBalancer = function(status){  
  conf = db.getSisterDB("config");
  var balancer_not_find = true;
  var balancer_stopped = false;
  var ret = conf.settings.find({_id:'balancer'}, {'stopped':1}).forEach(function(item){
    balancer_not_find = false;
    balancer_stopped = item.stopped;
  });
  //if not found insert one
  if (balancer_not_find){
    conf.settings.insert({"_id":"balancer", "stopped":false});
  }
  
  var balancer_status = balancer_stopped ? 'off' : 'on';

  if (typeof(status) != 'undefined'){
    status = !!status;
    if (status == balancer_stopped){
      print('Balancer is ' + balancer_status +' before');
      balancer_stopped = !balancer_stopped;
      conf.settings.update({_id:'balancer'}, {$set:{stopped:balancer_stopped}});
      balancer_status = balancer_stopped ? 'off' : 'on';
    }
  }
  print('Balancer is ' + balancer_status +' now');
}





nv.extend(DB, 'nvBalancer', nv.nvBalancer, 'nvBalancer([status]) - get or set balancer status, status value is 1 or 0');

