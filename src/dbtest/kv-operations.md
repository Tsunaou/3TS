# KV操作支持的接口

## 已有接口
> The current program supports the following commands: begin t1.get(k) t1.put(k,v) t1.getpred(v) t1.putpred(v,v) t1.vinc(k,+100) commit rollback

|操作|等价SQL|
|-|-|
|begin            |begin|
|t1.get(*)        |select * from t1|
|t1.get(k)        |select * from t1 where k=k|
|t1.put(k,v)      |insert into t1 values (k, v)|
|t1.getpred(v)    |select * from t1 where v=v|
|t1.putpred(v,v)  |update t1 set v=v where v=v|
|t1.vinc(k,+100)  |update t1 set v=v+100 where k=k|
|commit           |commit|
|rollback         |rollback|

## 谓词需新增接口
|操作|例子|等价SQL|
|-|-|-|
|t1.select(pred)|t1.select(k>0&&k<2)|select * from t1 where k>0 and k<2|
|t1.delete(pred)|t1.delete(k=3)| delete from t1 where k=3|
|t1.update(pred, v)|t.update(k>2, 2)|update t1 set v=2 where k>2|