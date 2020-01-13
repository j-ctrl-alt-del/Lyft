/* The below libraries represent each of the individual schema found in the Lyft dataset. 
The Lyft Level 5 dataset can be downloaded at https://level5.lyft.com/dataset/. */

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


/* The below sequence of data, sql, and merge steps systematically stitch together all 
pertinent schema for analysis. The SAS JSON engine, used above, divides each schema into 
a number of tables that must be appropiately merged for use. */

/***First merging the tables of the camera_intrinsic schema.***/

/** The values in element3 represent either the Optical Center variables (cx or cy) or the skew 
coefficient.  The range for cx values is above 600, meaning that anythng above 599 is a cx value.
The range for cy values is between 1 and 599.  Anything that is a 1 is a skew coefficient. **/
data cam_int_clean;
	set zcalsen.camera_intrinsic;
	if element3>=599 then cx=element3;
		else if 1<element3<599 then cy=element3;
			else if element3<=1 then do;
										cx=.;
										cy=.;
									end;
	if element1=0 then element1=.;
	if element2=0 then element2=.;
	by ordinal_root;
run;

proc sql; 
	create table cam_int_merged as
	select ordinal_root, element2 as focal_length, max(cy) as cy, max(cx) as cx
	from cam_int_clean
	group by ordinal_root
	;
run;

data cam_int_final;
	set cam_int_merged;
	where focal_length^=.;
run;

data calsenMerge;
	merge cam_int_final zcalsen.rotation zcalsen.translation zcalsen.root;
	by ordinal_root;
run;

/***Then merging the calibrated_sensor and sensor schema into senMerge.***/

/** Left-justifying the sensor schema. Otherwise, modality and channel will not show in 
resulting merge. **/
data work.senSorted;
	set zsensor.root;
	modality = left(modality);
	channel = left(channel);
run;

/** Sorting schema by sensor_token **/
proc sort data=work.senSorted out=work.senSorted;
	by token;
run;

proc sort data=work.calsenMerge out=calsenSorted;
	by sensor_token;
run;

/** Merging the sensor and calibrated_sensor schema on sensor_token. **/
data work.senMerge;
	merge work.calsenSorted work.senSorted (rename=(token=sensor_token));
	by sensor_token;
run;

/***Then merging senMerge and sample_data into senAndSampleDMerge***/

/** Left-justifying the sensor schema. Otherwise, listed variables will not show in 
resulting merge. **/
data work.smplDSorted;
	set zsmpl_d.root;
	rename = (timestamp=sample_data_timestamp);
	is_key_frame = left(is_key_frame);
	timestamp = left(timestamp);
	width = left(width);  
	height = left(height);
run;

/** Sorting the sample_data schema by calibrated_sensor_token. **/
proc sort data=work.smplDSorted out=work.smplDSorted;
	by calibrated_sensor_token;
run;

proc sort data=work.senMerge out=work.senMerge;
	by token;
run;

/** Merging the sensor data and sample_data schema on calibrated_sensor_token. **/
data work.senAndSampleDMerge;
	merge smplDSorted work.senMerge (rename=(token=calibrated_sensor_token));
	by calibrated_sensor_token;
run;


/** Formatting and merging the ego_pose schema. Otherwise, listed variables will be confused in 
future datasets. **/
data work.egoPoseTranslation;
	set zegopose.translation;
	rename translation1=ep_translation1;
	rename translation2=ep_translation2;
	rename translation3=ep_translation3;
run;

data work.egoPoseRotation;
	set zegopose.rotation;
	rename rotation1=ep_rotation1;
	rename rotation2=ep_rotation2;
	rename rotation3=ep_rotation3;
	rename rotation4=ep_rotation4;
run;

data work.egoPoseRoot;
	set zegopose.root;
run;

/** Sorting the ego_pose schema by ordinal_root in preparation for merge. **/
proc sort data=work.egoPoseTranslation out=work.egoPoseTranslationSorted;
	by ordinal_root;
run;

proc sort data=work.egoPoseRotation out=work.egoPoseRotationSorted;
	by ordinal_root;
run;

proc sort data=work.egoPoseRoot out=work.egoPoseRootSorted;
	by ordinal_root;
run;

/** Merging all ego_pose schema on ordinal_root. **/
data work.egoPoseMerge;
	merge egoPoseRoot egoPoseRotation egoPoseTranslation;
	by ordinal_root;
	rename token=ego_pose_token;
run;
  
  
/** Sorting senAndSampleDMerge by ego_pose_token. **/
proc sort data=work.senAndSampleDMerge out=work.senAndSampleDMerge;
	by ego_pose_token;
run;

proc sort data=work.egoPoseMerge out=work.egoPoseMerge;
	by ego_pose_token;
run;

/** Merging sensor data (senAndSampleDMerge) and sample_data on calibrated_sensor_token. **/
data work.senSampleDEgoMerge;
	merge senAndSampleDMerge work.egoPoseMerge;
	by ego_pose_token;
run;

/***Then merging senSampleDEgoMerge and sample schema into senAndSampleMerge.***/

/** Renaming timestamp so that later we can tell which timestamp is in use. **/
/** Left-justifying sample_timestamp in order to show that variable in the resulting merge. **/
data work.smplSorted;
	set zsmpl.root;
	rename timestamp=sample_timestamp;
	rename token=sample_token;
	sample_timestamp = left(sample_timestamp);
run;

/** Sorting both datasets on the same token. **/
proc sort data=work.senSampleDEgoMerge out=work.senSampleDEgoMerge;
	by sample_token;
run;

proc sort data=work.smplSorted out=work.smplSorted;
	by sample_token;
run;

/** Merging sample_data schema and senAndSampleDMerge on calibrated_sensor_token. **/
data work.senAndSampleMerge;
	merge work.senSampleDEgoMerge work.smplSorted;
	by sample_token;
run;

/***Then merging all sample_annotation schema***/

/** Formatting and merging the smpl_aTranslation schema. Otherwise, listed variables will be 
confused in future datasets. **/
data work.smpl_aTranslation;
	set zsmpl_a.translation;
	rename translation1=local_translation1;
	rename translation2=local_translation2;
	rename translation3=local_translation3;
run;

data work.smpl_aRotation;
	set zsmpl_a.rotation;
	rename rotation1=local_rotation1;
	rename rotation2=local_rotation2;
	rename rotation3=local_rotation3;
	rename rotation4=local_rotation4;
run;

data work.smpl_aRoot;
	set zsmpl_a.root;
	rename token = smpl_a_Token;
run;

/** Sorting the smpl_a schema by ordinal_root in preparation for merge. **/
proc sort data=work.smpl_aTranslation out=work.smpl_aTranslation;
	by ordinal_root;
run;

proc sort data=work.smpl_aRotation out=work.smpl_aRotation;
	by ordinal_root;
run;

proc sort data=work.smpl_aRoot out=work.smpl_aRoot;
	by ordinal_root;
run;

/** Merging all smpl_a schema on ordinal_root. **/
data work.smpl_aMerge;
	merge smpl_aRoot smpl_aRotation smpl_aTranslation;
	by ordinal_root;
run;


/** Sorting both datasets on the same token. **/
proc sort data=work.smpl_aMerge out=work.smpl_aMerge;
	by sample_token;
run;

proc sort data=work.senAndSampleMerge out=work.senAndSampleMerge;
	by sample_token;
run;

/** Merging sample_data schema and senAndSampleDMerge on calibrated_sensor_token. **/
data work.allSampleMerge;
	merge work.senAndSampleMerge work.smpl_aMerge;
	by sample_token;
run;



/***Finally merging allSampleMerge and instance schema into question2Merge***/

/** Renaming timestamp so that later we can tell which timestamp is in use. **/
/** Left-justifying nbr_annotations so that variable will successfully show in the resulting merge.**/
data work.instanceSorted;
	set zinstnce.root;
	rename token=instance_token;
	nbr_annotations = left(nbr_annotations);
run;

/** Sorting both datasets on the same token. **/
proc sort data=work.instanceSorted out=work.instanceSorted;
	by instance_token;
run;

proc sort data=work.allSampleMerge out=work.allSampleMerge;
	by instance_token;
run;

/** Merging sample_data schema and senAndSampleDMerge on calibrated_sensor_token. **/
data work.question2Merge;
	merge work.allSampleMerge work.instanceSorted;
	by instance_token;
run;


/***Then merging question2Merge and the root table of the scene schema into question2MergeScene.***/

/** Using a data step because SAS produces an error if I attempt to read directly from the table. **/
data work.sceneSorted;
	set zscene.root;
	rename token=scene_token;
run;

/** Sorting both datasets on the same token. **/
proc sort data=work.question2Merge out=work.question2Merge;
	by scene_token;
run;

proc sort data=work.sceneSorted out=work.sceneSorted;
	by scene_token;
run;

/** Merging scene schema and question2Merge on calibrated_sensor_token. **/
data work.question2MergeScene;
	merge work.question2Merge work.sceneSorted;
	by scene_token;
run;


/***Then merging question2MergeScene and the log table of the scene schema into question2MergeScene.***/

/** Using a data step because SAS produces an error if I attempt to read directly from the table. **/
data work.logSorted;
	set zlog.root;
	rename token=log_token;
run;

/** Sorting both datasets on the same token. **/
proc sort data=work.question2MergeScene out=work.question2MergeScene;
	by log_token;
run;

proc sort data=work.logSorted out=work.logSorted;
	by log_token;
run;

/** Merging sample_data schema and senAndSampleDMerge on calibrated_sensor_token. **/
data work.question2MergeSceneLog;
	merge work.question2MergeScene work.logSorted;
	by log_token;
	/** Using this data step to convert the UNIX datetime to human-readable UTC datetime. **/
	format converted_time datetime25.;
	converted_time = dhms('01jan1970'd,0,0,timestamp/1000000);
run;



/***The above step ends normal merging of the schema.  Below is a mixture of commands
run to explore the data and more steps to seperate the data into logical segments for analysis.***/


/***The below is material for figures 8-12.  Material for figures 5-7 is further below.***/

/** Figure 8 Starts Here - Running basic exploratory measures. **/
proc means data=work.question2MergeSceneLog n min max range;
	var nbr_annotations height width focal_length cx cy; *ep_translation1 ep_translation2 ep_translation3;
run;

proc means data=work.question2MergeSceneLog mean std skewness;
	var nbr_annotations height width focal_length cx cy;
run;


/** Showing low r-squared value when attempting to run a normal linear regression. **/
proc reg data=work.question2MergeSceneLog;
  model nbr_annotations = height width focal_length cx cy / selection=adjrsq  aic sbc bic cp;
run;

/** However, showing high significance for each variable. **/
proc reg data=work.question2MergeSceneLog;
  model nbr_annotations = height focal_length cx cy;
run;


/** Looking at the discrete values for each variable as shown in the 'Class Level Information' chart. **/
proc glm data=work.question2MergeSceneLog;
	class width focal_length cx cy;
  	model nbr_annotations = width focal_length cx cy;
run;

/** Using hpbin to create 10 bins for each variable to do more data exploration. **/
proc hpbin data=work.question2MergeSceneLog output=ques2Binned numbin=10;                
   input focal_length cx cy;
   id token nbr_annotations sample_token instance_token smpl_a_Token vehicle focal_length cx cy ep_translation1 ep_translation2 ep_translation3;
run;

/** Again showing discrete values for each variable as shown in the 'Class Level Information' chart. **/
ods graphics off;
proc glm data=work.question2MergeSceneLog;
  class height width focal_length cx cy;
  model nbr_annotations = height|width|focal_length|cx|cy / solution;
run;


/** Seeing if CAM_FRONT_ZOOMED is significantly affecting the height/width of images.  
This was hinted at in the discrete values above. **/
proc format;
	value $zoom
		'CAM_FRONT_ZOOMED' = 'CAM_FRONT_ZOOMED'
		'CAM_BACK','CAM_BACK_LEFT','CAM_BACK_RIGHT',
		'CAM_FRONT','CAM_FRONT_LEFT','CAM_FRONT_RIGHT','LIDAR_TOP'= 'Not CAM_FRONT_ZOOMED' 
		;
run;

/** The below frequency charts show that the height and width of CAM_FRONT_ZOOMED images is 
distinct from all other images. **/
proc freq data=work.question2MergeSceneLog;
	table channel*height;
	format channel $zoom.;
run;

proc freq data=work.question2MergeSceneLog;
	table channel*width;
	format channel $zoom.;
run;



/***Dividing the dataset into not CAM_FRONT_ZOOMED and CAM_FRONT_ZOOMED sections.
This step is taken in order to seperate metadata for images of different sizes. 
The CAMFRONTZOOMEDdata metadata is related to images with height 864 and width 2048. 
The notCAMFRONTZOOMEDdata metadata is related to images with height 1024 and width 1224.***/

data notCAMFRONTZOOMEDdata;
	set work.question2MergeSceneLog;
	where channel ne 'CAM_FRONT_ZOOMED';
run;

data CAMFRONTZOOMEDdata;
	set work.question2MergeSceneLog;
	where channel eq 'CAM_FRONT_ZOOMED';
run;

/** Looking exclusively at notCAMFRONTZOOMEDdata.  Again, there is a low adjusted r-squared value. **/
proc reg data=work.notCAMFRONTZOOMEDdata;
  model nbr_annotations = focal_length cx cy / selection=adjrsq  aic sbc bic cp;
run;


/* Using hpbin to create 10 bins for each variable to do data exploration. The below displays a widely varying proportion 
of responses for each focal length range. */
proc hpbin data=work.notCAMFRONTZOOMEDdata output=ques2NotCAMBinned numbin=50;                
   input focal_length cx cy ep_translation1 ep_translation2 ep_translation3;
   id token nbr_annotations sample_token instance_token smpl_a_Token vehicle focal_length cx cy ep_translation1 ep_translation2 ep_translation3 date_captured;
run;



/** Figures 9-11 Start Here - Creating heat maps of vehicle frequency for each sensor.**/
proc freq data=work.ques2NotCAMBinned;
	table vehicle*BIN_focal_length / out=veh_vs_fl;
run;

data veh_vs_fl;
	set veh_vs_fl;
	by BIN_focal_length;
run;

proc sgplot data=work.veh_vs_fl;
   heatmap x=BIN_focal_length y=vehicle / freq=Count 
         discretex discretey
         colormodel=TwoColorRamp outline;
run;

proc freq data=work.ques2NotCAMBinned;
	table vehicle*BIN_cy / out=veh_vs_cy;
run;

data veh_vs_cy;
	set veh_vs_cy;
	by BIN_cy;
run;

proc sgplot data=work.veh_vs_cy;
   heatmap x=BIN_cy y=vehicle / freq=Count 
         discretex discretey
         colormodel=TwoColorRamp outline;
run;

proc freq data=work.ques2NotCAMBinned;
	table vehicle*BIN_cx / out=veh_vs_cx;
run;

data veh_vs_cx;
	set veh_vs_cx;
	by BIN_cx;
run;

proc sgplot data=work.veh_vs_cx;
   heatmap x=BIN_cx y=vehicle / freq=Count 
         discretex discretey
         colormodel=TwoColorRamp outline;
run;

/**Running tukey adjustments to quantify differences in nbr_annotations**/
ods graphics off;
proc glm data=work.ques2NotCAMBinned; 
  class BIN_focal_length;
  model nbr_annotations = BIN_focal_length / solution; 
  lsmeans BIN_focal_length / diff adjust=tukey;
run;

ods graphics off;
proc glm data=work.ques2NotCAMBinned; 
  class BIN_cx;
  model nbr_annotations = BIN_cx / solution; 
  lsmeans BIN_cx / diff adjust=tukey;
run;

ods graphics off;
proc glm data=work.ques2NotCAMBinned; 
  class BIN_cy;
  model nbr_annotations = BIN_cy / solution; 
  lsmeans BIN_cy / diff adjust=tukey;
run;

/**Creating histograms detailing the number of annotations vs each camera intrinsic**/
proc sgplot data=work.ques2NotCAMBinned;
	vbar BIN_focal_length / response=nbr_annotations stat=mean;
run;

proc sgplot data=work.ques2NotCAMBinned;
	vbar BIN_cx / response=nbr_annotations stat=mean;
run;

proc sgplot data=work.ques2NotCAMBinned;
	vbar BIN_cy / response=nbr_annotations stat=mean;
run;

/* Using hpbin to create 50 bins for each variable for generalized linear model analysis and the first logistic analysis.  
Expanding beyond 10 bins here gives more precise results and is practical here. */
proc hpbin data=work.notCAMFRONTZOOMEDdata output=ques2NotCAMBinned50 numbin=50;                
   input focal_length cx cy ep_translation1 ep_translation2 ep_translation3;
   id token nbr_annotations sample_token instance_token smpl_a_Token vehicle focal_length cx cy ep_translation1 ep_translation2 ep_translation3 date_captured;
run;

/**Running a generalized linear model on the data.  This shows that focal_length, cx, and cy are all significant (p<.0001). 
This is reinforced by the significant chi-squared value.**/
proc genmod data=work.ques2NotCAMBinned50;
	class date_captured vehicle BIN_ep_translation1 BIN_ep_translation2; 
	model nbr_annotations = focal_length cx cy / type3;
	Repeated subject=date_captured*vehicle*BIN_ep_translation1*BIN_ep_translation2  / sorted;
run;

/**Using proc format to seperate relatively low values (0-50) and relatively high values (51-126)**/
proc format;
	value numann
		0-50 = '0-50'
		51-126 = '51-126'
		;
run;

/**The PROC LOGISTIC here shows that all the combinations of values, with the exception of focal length and cy, are significant.**/
proc logistic data=work.ques2NotCAMBinned50;
   model nbr_annotations = BIN_focal_length | BIN_cx | BIN_cy;
   format nbr_annotations numann.;
   store logiModel;
run;


/**The PROC GLMSELECT below show which combinations of binned values produces more annotations.  10 bins is used again here for easy analysis with previous results 
in figures 9-11.**/
proc glmselect data=work.ques2NotCAMBinned;
  class BIN_focal_length BIN_cx BIN_cy;
  model nbr_annotations = BIN_focal_length|BIN_cx|BIN_cy @3;
run;

proc glmselect data=work.ques2NotCAMBinned;
  class BIN_focal_length BIN_cx BIN_cy;
  model nbr_annotations = BIN_focal_length|BIN_cx|BIN_cy @2;
run;



/***The below are for figures 5 through 7***/

/*Using hpbin to create 50 bins for each variable to do data exploration*/
proc hpbin data=work.notCAMFRONTZOOMEDdata output=ques2NotCAMBinned50 numbin=50;                
   input focal_length cx cy ep_translation1 ep_translation2 ep_translation3;
   id token nbr_annotations sample_token instance_token smpl_a_Token vehicle focal_length cx cy 
   		ep_translation1 ep_translation2 ep_translation3 date_captured;
run;

/**Looking at the number of entries on each date captured for each vehicle**/
proc freq data=ques2NotCAMBinned50;
	table date_captured*vehicle / out=date_vs_veh;
run;

proc sort data=work.date_vs_veh out=work.date_vs_veh;
	by date_captured;
run;

data date_vs_veh;
	set date_vs_veh;
	by vehicle;
run;

/** Figure 5 - The below graph shows more vehicles progressively being introduced as the dataset ages. **/
ods graphics on / width=9in height=6in;
proc sgplot data=work.date_vs_veh;
   heatmap x=date_captured y=vehicle / freq=Count 
         discretex discretey
		 colormodel=TwoColorRamp outline;
run;

/** Figure 6 - Showing the total number of annotations for all vehicles. **/
proc sgplot data=ques2NotCAMBinned50;
	heatmap x=BIN_ep_translation1 y=BIN_ep_translation2 / colorresponse=nbr_annotations colorstat=sum xbinsize=1 ybinsize=1;
   	xaxis values = (0 to 50 by 1);
   	yaxis values = (0 to 50 by 1);
   	gradlegend;
run;

/** Figure 7 - Using a macro to create graphs for the total number of annotations associated with each vehicle. **/
%macro CarMaps;
	%let cars = "a004" "a005" "a006" "a007" "a008" "a009" "a011" "a012" "a015" "a017";
	%local i next_car;
	%do i=1 %to %sysfunc(countw(&cars));
		%let car = %scan(&cars, &i);
		proc sgplot data=ques2NotCAMBinned50(where=(vehicle=&car));
   			heatmap x=BIN_ep_translation1 y=BIN_ep_translation2 / colorresponse=nbr_annotations colorstat=sum xbinsize=1 ybinsize=1;
   			xaxis values = (0 to 50 by 1);
   			yaxis values = (0 to 50 by 1);
   			gradlegend;
   			title 'The Number of Annotations Captured on '&car;
		run;
	%end;
%mend;

%CarMaps;

/** An extra macro showing the number of annotations captured on an individual day. **/
%macro DateMaps;
	%let dates = 2019-01-02|2019-01-07|2019-01-09|2019-01-11|2019-01-24|2019-01-25|2019-01-28|2019-01-29|2019-01-30|2019-01-31|2019-02-01
					|2019-02-05|2019-02-06|2019-02-07|2019-02-08|2019-02-11|2019-02-12|2019-02-13|2019-02-17|2019-02-18|2019-02-19|2019-02-20
					|2019-02-28|2019-03-04|2019-03-06|2019-03-07|2019-03-08|2019-03-22|2019-03-25;
	%local i next_date;
	%do i=1 %to %sysfunc(countw(&dates));
		%let date = %scan(&dates, &i, |);
		proc sgplot data=ques2NotCAMBinned50(where=(date_captured="&date"));
   			heatmap x=BIN_ep_translation1 y=BIN_ep_translation2 / colorresponse=nbr_annotations colorstat=sum xbinsize=1 ybinsize=1;
   			xaxis values = (0 to 50 by 1);
   			yaxis values = (0 to 50 by 1);   			
   			gradlegend;
   			title "The Number of Annotations Captured on &date";
		run;
	%end;
%mend;

ods exclude none;
options mprint;
%DateMaps;



