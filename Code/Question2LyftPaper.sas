libname zattrb json '/data1/AutonVeh/v1.02-train/v1.02-train/attribute.json';
libname zcalsen json '/data1/AutonVeh/v1.02-train/v1.02-train/calibrated_sensor.json';
libname zcatgry json '/data1/AutonVeh/v1.02-train/v1.02-train/category.json';
libname zegopose json '/data1/AutonVeh/v1.02-train/v1.02-train/ego_pose.json';
libname zinstnce json '/data1/AutonVeh/v1.02-train/v1.02-train/instance.json';
libname zlog json '/data1/AutonVeh/v1.02-train/v1.02-train/log.json';
libname zmap json '/data1/AutonVeh/v1.02-train/v1.02-train/map.json';
libname zsmpl json '/data1/AutonVeh/v1.02-train/v1.02-train/sample.json';
libname zsmpl_a json '/data1/AutonVeh/v1.02-train/v1.02-train/sample_annotation.json';
libname zsmpl_d json '/data1/AutonVeh/v1.02-train/v1.02-train/sample_data.json';
libname zscene json '/data1/AutonVeh/v1.02-train/v1.02-train/scene.json';
libname zsensor json '/data1/AutonVeh/v1.02-train/v1.02-train/sensor.json';
libname zvisib json '/data1/AutonVeh/v1.02-train/v1.02-train/visibility.json';

/**Getting the pertinent columns from the log and scene schemas**/
proc sql;
	create table instanceSchema as
	select token as instance_token, category_token, first_annotation_token, last_annotation_token, nbr_annotations
	from zinstnce.root
	;
quit;

proc sql;
	create table categorySchema as
	select token as category_token, name as category_name
	from zcatgry.root
	;
quit;

proc sql;
	create table instanceCategory as
	select *
	from instanceSchema, categorySchema
	where instanceSchema.category_token eq categorySchema.category_token
	;
quit;
	

/*Left-justifying the sample_annotation schema (both root and attribute_tokens tables). 
Otherwise, the merge will not be successful because the ordinal_root is right justified.*/
data work.smpl_a_attr_tokens;
	set zsmpl_a.attribute_tokens;
	ordinal_root = left(ordinal_root);
run;

data work.smpl_a_root;
	set zsmpl_a.root;
	rename next=smpl_a_next;
	rename prev=smpl_a_prev;
	rename token=smpl_a_token;
	ordinal_root = left(ordinal_root);
run;

data work.smpl_a_size;
	set zsmpl_a.size;
	rename size1=sizeHeight;
	rename size2=sizeWidth;
	rename size3=sizeDepth;
	ordinal_root = left(ordinal_root);
run;

/**Sorting sample_annotation schema by ordinal_root and merging**/
proc sort data=work.smpl_a_attr_tokens out=work.smpl_a_attr_tokens2;
	by ordinal_root;
run;

proc sort data=work.smpl_a_root out=work.smpl_a_root2;
	by ordinal_root;
run;

proc sort data=work.smpl_a_size out=work.smpl_a_size;
	by ordinal_root;
run;

/**None of the variables listed in the DROP statement have meaningful data.**/
data sampleAnnSchema(DROP=num_lidar_pts num_radar_pts visibility_token);
	merge work.smpl_a_attr_tokens work.smpl_a_root work.smpl_a_size;
	by ordinal_root;
run;

proc sql;
	create table smplAInstance as
	select *
	from work.sampleAnnSchema, work.instanceCategory
	where sampleAnnSchema.instance_token eq instanceCategory.instance_token;
quit;

data work.attribute(DROP=description);
	set zattrb.root;
	rename token=attribute_token;
	rename name=attribute_name;
	ordinal_root = left(ordinal_root);
run;

/**Note with the below that many entries have more than one attribute token.**/
proc sql;
	create table smplAInstAttr as
	select *
	from work.attribute, work.smplAInstance
	where attribute.attribute_token eq smplAInstance.attribute_tokens1 or 
		attribute.attribute_token eq smplAInstance.attribute_tokens2;
quit;

data work.smpl_d_root;
	set zsmpl_d.root;
	rename next=smpl_d_next;
	rename prev=smpl_d_prev;
	rename token=smpl_d_token;
	rename timestamp=smpl_d_timestamp;
	ordinal_root = left(ordinal_root);
run;

proc sql;
	create table smplADataInstAttr as
	select *
	from work.smplAInstAttr, work.smpl_d_root
	where smplAInstAttr.sample_token eq smpl_d_root.sample_token;
quit;

/***This is the end of merging.***/



/** Figure 13 - Conducting a proporitional odds assesment to show how different dimensions affect vehicle identification.**/

proc format;
  value $object_type
    'animal','pedestrian'='Not Vehicle'
    'bicycle','bus','car','emergency_vehicle','motorcycle',
    'other_vehicle','truck'='Vehicle'
    ;
run;

proc logistic data=work.smplADataInstAttr descending;
  format category_name $object_type.;
  model category_name = sizeHeight|sizeWidth|sizeDepth / unequalslopes;
run;


/** Figure 14 - Showing how attributes (object actions) correlate with object identification. **/
ods graphics / reset width=11.4in height=4.8in imagemap MAXOBS=6528345;
proc sgplot data=smplADataInstAttr;
	heatmap x=attribute_name y=category_name / name='HeatMap';
	gradlegend 'HeatMap';
run;ods graphics / reset;

ods graphics off;
proc logistic data=work.smplADataInstAttr;
  class attribute_name;
  model category_name = nbr_annotations attribute_name / link=glogit;
run;