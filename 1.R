ori_data <- read.table("1.txt",header = TRUE,fileEncoding="utf-8", fill = TRUE)
city <- unique(ori_data$city)
city_cnt <- length(city)
final_data <- as.data.frame(matrix(0,city_cnt,3))
names(final_data) <- c("city","real_ratio","real_abnormal_ratio")
final_data$city <- city

for (j in 1:city_cnt){
  temp <- ori_data[ori_data$city==city[j],]  ##先把第j个城市从ori_data里面取出来
  ## temp就是一个文件中的一个城市的全部数据
  n1 <- dim(temp)[1]
  temp <- temp[temp$imei != "",]      ## 处理异常数据
  temp <- temp[temp$imei !="undefined",]
  temp <- temp[temp$line_quality_flag != "NULL",]
  ## 把temp中的factor整理好
  # temp$line_id <- droplevels(temp$line_id)
  temp$imei <- droplevels(temp$imei)
  temp$line_quality_flag <- as.numeric(as.character(temp$line_quality_flag))
  temp$req_time <- as.numeric(as.character(temp$req_time))
  
  n2 <- dim(temp)[1]
  print(paste(city[j],paste("异常数据的行数：",n1-n2,sep = ""),sep = ""))
  #### 把lq后面还是lq的lq删除然后再处理, 先过滤一遍lq在处理
  flag_index <- which(temp$action==1)  ## length(flag_index)的值为89193
  ## flag_index是lq的位置在temp矩阵中的下标
  n_max <- n2  # n2是temp的行数

  if (max(flag_index) < n_max)
  {
      lq_lineid_imei_reqtime1 <- temp[flag_index,c("line_id","imei","req_time")]
      lq_lineid_imei_reqtime2 <- temp[flag_index+1,c("line_id","imei","req_time","action")]
  } else
  {
      flag_index <- flag_index[1:(length(flag_index)-1)]
      lq_lineid_imei_reqtime1 <- temp[flag_index,c("line_id","imei","req_time")]
      lq_lineid_imei_reqtime2 <- temp[flag_index+1,c("line_id","imei","req_time","action")]
  }

  
  check_lq_flag_in12 <- lq_lineid_imei_reqtime2$action==1
  check_lq_index_in12 <- which(lq_lineid_imei_reqtime2$action==1)
  ## 通过把invalid_lq_flag_in12里面的一些TURE改为FASLE,最后flag_index[invalid_lq_flag_in12]就是那些无用lq的下标
  imei_conditon <- lq_lineid_imei_reqtime1[check_lq_flag_in12,"imei"] == lq_lineid_imei_reqtime2[check_lq_flag_in12,"imei"]
  lineid_condition <- lq_lineid_imei_reqtime1[check_lq_flag_in12,"line_id"] == lq_lineid_imei_reqtime2[check_lq_flag_in12,"line_id"]
  reqtime_condition <- (lq_lineid_imei_reqtime2[check_lq_flag_in12,"req_time"]-lq_lineid_imei_reqtime1[check_lq_flag_in12,"req_time"]) < 60
  
  invalid_lq_index_in12 <- check_lq_index_in12[imei_conditon & lineid_condition & reqtime_condition]
  invalid_lq_flag_in12 <- rep(FALSE,times=length(check_lq_flag_in12))
  invalid_lq_flag_in12[invalid_lq_index_in12] <- TRUE
  print(paste(city[j],paste("无效查询的个数：",length(invalid_lq_index_in12),sep = ""),sep = ""))
  ## sum(invalid_lq_flag_in12 & check_lq_flag_in12)的值和length(invalid_lq_index_in12)一致
  
  # 更新temp
  invalid_lq_index <- flag_index[invalid_lq_flag_in12]
  if(length(invalid_lq_index)>0){
    temp <- temp[-invalid_lq_index,]
  }
  # temp$line_id <- droplevels(temp$line_id)
  temp$imei <- droplevels(temp$imei)
  # temp$line_quality_flag <- droplevels(temp$line_quality_flag)
  ## 更新temp的行数
  n_max <- dim(temp)[1]
  ## 更新flag_index为temp中实时lq的行标
  flag_index <- which(temp$action==1 & temp$line_quality_flag == 1)

  rm(lq_lineid_imei_reqtime1)
  rm(lq_lineid_imei_reqtime2)
  
  ## lq的个数
  final_data[final_data$city==city[j],"real_ratio"] <- round(sum(temp$action==1 & temp$line_quality_flag == 1)/(sum(temp$action==1)+0.001),4)*100

  ## 匹配实时lq之后的lbl
  if (max(flag_index) < n_max) 
    {
      lq_lineid_imei_reqtime <- temp[flag_index,c("line_id","imei","req_time")]  ## 所有实时查询的记录
      lbl_lineid_imei_reqtime <- temp[flag_index+1,c("line_id","imei","req_time","action","line_quality_flag")]
    } else 
    {
      flag_index <- flag_index[1:(length(flag_index)-1)]
      lq_lineid_imei_reqtime <- temp[flag_index,c("line_id","imei","req_time")]
      lbl_lineid_imei_reqtime <- temp[flag_index+1,c("line_id","imei","req_time","action","line_quality_flag")]
    }
  l <- dim(lq_lineid_imei_reqtime)[1]
  lq_lineid_imei_reqtime$"lbl_req_time" <- rep(0,l)
  lq_lineid_imei_reqtime$"lbl_line_quality_flag" <- rep(0,l)
  ## 在lq_lineid_imei_reqtime后面加上两列用来匹配相应的lbl的值
  
  imei_conditon <- lq_lineid_imei_reqtime$imei == lbl_lineid_imei_reqtime$imei
  lineid_condition <- lq_lineid_imei_reqtime$line_id == lbl_lineid_imei_reqtime$line_id
  reqtime_condition <- (lbl_lineid_imei_reqtime$req_time-lq_lineid_imei_reqtime$req_time) < 60
  
  match_flag <- imei_conditon & lineid_condition & reqtime_condition
  lq_lineid_imei_reqtime$lbl_req_time[match_flag] <- lbl_lineid_imei_reqtime$req_time[match_flag]
  lq_lineid_imei_reqtime$lbl_line_quality_flag[match_flag] <- as.numeric(as.character(lbl_lineid_imei_reqtime$line_quality_flag[match_flag]))
  lq_lineid_imei_reqtime$lbl_req_time[!match_flag] <- 0
  lq_lineid_imei_reqtime$lbl_line_quality_flag[!match_flag] <- 10
  
  ## 其他字段的赋值
  final_data[final_data$city==city[j],"real_abnormal_ratio"] <- round((sum(lq_lineid_imei_reqtime$lbl_line_quality_flag == 2)+sum(lq_lineid_imei_reqtime$lbl_line_quality_flag == 3)+sum(lq_lineid_imei_reqtime$lbl_line_quality_flag == 4))/(sum(temp$action==1 & temp$line_quality_flag == 1)+0.01),4)*100
print(paste(city[j],"已完成",sep=" "))
}
print("全部数据已完成!")
names(final_data)[1] <- "city_id"
## 筛选城市
data1 <- final_data[,c(1,2)]
data2 <- final_data[!final_data$city_id %in% c(4,7),c(1,3)]
#data2 <- final_data[,c(1,3)]
write.table(data1,file="2.txt",sep = "\t",fileEncoding = "utf-8",row.names = FALSE,col.names = TRUE,quote = FALSE)
write.table(data2,file="3.txt",sep = "\t",fileEncoding = "utf-8",row.names = FALSE,col.names = TRUE,quote = FALSE)
