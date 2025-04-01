# Python Adopter 数据一体机（数据全量、数据增量、数据修复）

## 功能
python Adopter是基于Canal写的python版本，主要开发的功能如下:
<ul>
    <li>第一阶段：利用TCP和KAFKA做单表同步，且字段可以完全自我控制，只需要调整config文件下的mapConfig.json,设置是甚至字段默认值。</li>
    <li>第二阶段：数据全量迁移，搭配增量可以做数据迁移。</li>
    <li>第三阶段：做数据修复，用两个一个是基于kettle的比对修复和利用binlog重复消费的修复。</li>
    <li>前三个阶段都一定是做完的。第四阶段的话会根据用户量选择完成，数据库统一检查方案，返回报告，帮助DBA去DEBUG数据库查询慢或者插入更新慢的问题。</li>
</ul>
<p>
目前完成的只有第一阶段，且会在两个大项目上进行测试，如果有任何问题都会及时修改，不然我要死嘞。
</p>
<p>
另外如果你有任何关于数据迁移方面的问题，都可以留言，我会尽快回复。
</p>

## 安装依赖
在pythonAdopter目录下执行以下命令<br/>
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple/ -r requirement.txt<br/>
linux pip替换成pip3
## 优势

优势在于:
<ol>
    <li>
        <p>无论是kafka模式还是TCP模式mysql的cursor和ACK位置和KAFKA坐标都是绝对绑定，即如果数据没有成功消费
    一定不会告诉canal数据已经成功消费了，尽可能的在消费端保证数据不要出现数据丢失的情况</p>
    </li>
    <li>
        <p>
            可以配置固定值，即如果目标表比源表字段多，你想要加入一个在本次数据传输中的固定值（不要问为什么有这个需求，因为
        我遇到了）
        </p>
    </li>
    <li>
        <p>如果你想要自定义自己的update insert和delete语句完全支持，你可以直接传入函参。</p>
    </li>
</ol>

## 后续开发计划

<ol>
    <li>后续还会加入数据数据基于binlog的修复方案。你只需要给我一个binlog的timestamp或者position就可以让
    特定的目标数据库重演这一部分操作,且也支持replace操作
    </li>
    <li>
    目前kettle的自动生成数据修复方案已经完成大部分了，但现在先不放出来了。
    </li>
</ol>

### 如果你在数据融合时遇到什么特殊的情况，留言。我会根据场景的常见性把他加入到自己的功能模块中

### 现在只是测试版，会在2024年1月中旬放出正式版配合所有的参数说明，调试方法也放到了main.py里面了。
### 额外功能说明:
#### findDiffColumns.py - 找到目标表和源表字段不一样的地方
<ol>
    <li>
    tableGroup ->定义哪些表需要检测
    </li>
    <li>
findDiffernt(originHost="220.179.5.197",originPort=8689,originUser="root",originPassword="Zkxbx@2011",originDatabase="xex_plus",
                 tableName=i,targetHost="220.179.5.197",targetPort=8689,targetUser="root",targetPassword="Zkxbx@2011",targetDatabase="civil_admin_aq")
    定义源库和目标库的连接方式
    </li>
</ol>

#### mapGeneration.py - 生成配置文件
<ul>
    <li>
        
        targetHost="220.179.5.197", //目标库连接方式
        targetPort=8689,//目标库port
        targetUser="root",//目标库账号
        targetPassword="Zkxbx@2011", //目标库密码
        targetDatabase="civil_admin_aq",//目标库名称
        originHost="220.179.5.197", //源库链接方式
        originPort=3306, //源库port
        originUser="root", //源库User
        originPassword="Zkxbx@2011", //源库密码
        originDatabase="xex_plus", //源库名称
        tables=["government_buys_services_apply"] //需要用到的表
    </li>
</ul>
