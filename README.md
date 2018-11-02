# FBDP_Lab_4
程序实现：
1. wordcount：(wordcount.java)

	在书本源码的基础上，在map函数中修改语：
	
		StringTokenizer itr = new StringTokenizer(value.toString()," \t\n\t\f,.:;?![ ]'");
		
	实现了对常见标点的切分，以此忽略各种常见标点符号。
	
	同时利用：
	
		word.set(itr.nextToken().toLowerCase());word.set(itr.nextToken().toLowerCase());
		
	将所有字母置为小写，实现了忽略大小写的功能。
	
2. 矩阵乘法参考了书本的源代码实现(matrix.java)

3. 关系代数的选择、投影、并集、交集、差集及自然连接的MapReduce实现 

（该部分代码参考了书本和https://blog.csdn.net/qq_38386316/article/details/81709690 ）
	
3.1. 选择关系的实现：(relation.java)

	首先程序定义了RelationA class，定义RelationA的四个属性：id, name, age, weight. 按行读取文件，并将读到的数据传递给RelationA. 
	
	在实现“age=18”中，定义了布尔类型函数isCondition，当满足：
	
			if(col ==2 && Integer.parseInt(value) == age)；（age是文件内读取到的年龄）
			
	函数返回true，否则返回false。
	
	执行bin/hadoop jar relation.jar relation.relation input output 2 18运行程序。其中，2是程序识别文件的列数，这里识别第三列“age”；18是使程序满足的条件值，并将18赋值给value。
	
	同样，要实现“age>18”时，只要修改Integer.parseInt(value) < age即可.

3.2. 投影关系的实现：(projection.java)

	主要借助
	
			context.write(new Text(record.getCol(col)), NullWritable.get());
			
	实现了对文件的第‘col’列的提取。这里col在程序执行时被赋值为1，即文件的第二列“name”列。
	
3.3. 并关系的实现：(union.java)

	主要通过判断IntWritable val的和是否大于0，大于0则通过
	
				context.write(key, NullWritable.get());
				
	写入

3.4. 交关系的实现：(intersection.java)

	与并关系类似，判断IntWriteable val的和如果等于2，则两个文件中均出现，通过context.write写入。
	
3.5. 差关系的实现：(difference.java)

	对文件Ra的每行数据作记号A，对Rb做记号B，形成键值对<line, A>, <line, B>. 在reduce函数中判断每一个line的值是否均为A：
	
					if (!val.toString().equals("A"))
					
	如果是，则写入；如果不是，则说明B文件中也有这条line的信息，不做处理，查看下一个line.
	
3.6. 自然连接关系的实现：(nujoin.java)

	Map函数实现将文件每一行转化为键值对，键为各成员id，值为文件每行的属性加上“A” “B”————对两个文件的区分，; 
	
	Reduce函数将对应键的值连接起来即可。
