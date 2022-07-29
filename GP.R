dp<-read.csv('C:/Temp/daily_prices_2015_2020.csv')
head(dp)
dp<-head(dp,-1)

require(RPostgres) # did you install this package?
require(DBI)
conn <- dbConnect(RPostgres::Postgres()
                  ,user="stockmarketreader"
                  ,password="read123"
                  ,host="localhost"
                  ,port=5432
                  ,dbname="stockmarketGP"
)

#custom calendar
qry='SELECT * FROM custom_calendar ORDER by date'
ccal<-dbGetQuery(conn,qry)
#eod prices and indices
qry1="SELECT symbol,date,adj_close FROM eod_indices WHERE date BETWEEN '2015-12-31' AND '2021-3-31'"
qry2="SELECT ticker,date,adj_close FROM eod_quotes WHERE date BETWEEN '2015-12-31' AND '2021-3-31'"
eod<-dbGetQuery(conn,paste(qry1,'UNION',qry2))
dbDisconnect(conn)
rm(conn)
#Explore
ccal<-tail(ccal,-365)
head(ccal)
tail(ccal)
nrow(ccal)

head(eod)
tail(eod)
nrow(eod)

tail(eod[which(eod$symbol=='SP500TR'),])  #select row num where symbol = SP500TR

tdays<-ccal[which(ccal$trading==1),,drop=F]
head(tdays)
nrow(tdays)-1

# Completeness ----------------------------------------------------------
# Percentage of completeness
pct<-table(eod$symbol)/(nrow(tdays)-1)
selected_symbols_daily<-names(pct)[which(pct>=0.99)]
eod_complete<-eod[which(eod$symbol %in% selected_symbols_daily),,drop=F]

#check
head(eod_complete)
tail(eod_complete)
nrow(eod_complete)

#YOUR TURN: perform all these operations for monthly data
#Create eom and eom_complete
#Hint: which(ccal$trading==1 & ccal$eom==1)

# Transform (Pivot) -------------------------------------------------------

require(reshape2) #did you install this package?
eod_pvt<-dcast(eod_complete, date ~ symbol,value.var='adj_close',fun.aggregate = mean, fill=NULL)
#check
eod_pvt[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt) # column count
nrow(eod_pvt)

# YOUR TURN: Perform the same set of tasks for monthly prices (create eom_pvt)

# Merge with Calendar. merge is inner join, by is on, all is left outer join-----------------------------------------------------
eod_pvt_complete<-merge.data.frame(x=tdays[,'date',drop=F],y=eod_pvt,by='date',all.x=T)

#check
eod_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

#use dates as row names and remove the date column
rownames(eod_pvt_complete)<-eod_pvt_complete$date
eod_pvt_complete$date<-NULL

#re-check
eod_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

head(eod_pvt_complete)
# Missing Data Imputation -----------------------------------------------------
# We can replace a few missing (NA or NaN) data items with previous data
# Let's say no more than 3 in a row...
require(zoo)
eod_pvt_complete<-na.locf(eod_pvt_complete,na.rm=F,fromLast=F,maxgap=3)
#re-check
eod_pvt_complete[1:10,1:5] #first 10 rows and first 5 columns 
ncol(eod_pvt_complete)
nrow(eod_pvt_complete)

# Calculating Returns -----------------------------------------------------
require(PerformanceAnalytics)
eod_ret<-CalculateReturns(eod_pvt_complete)

#check
eod_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eod_ret)
nrow(eod_ret)

#remove the first row
eod_ret<-tail(eod_ret,-1) #use tail with a negative value
#check
eod_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eod_ret)
nrow(eod_ret)

# YOUR TURN: calculate eom_ret (monthly returns)

# Check for extreme returns -------------------------------------------
# There is colSums, colMeans but no colMax so we need to create it
colMax <- function(data) sapply(data, max, na.rm = TRUE)
# Apply it
max_daily_ret<-colMax(eod_ret)
max_daily_ret[1:10] #first 10 max returns
# And proceed just like we did with percentage (completeness)
selected_symbols_daily<-names(max_daily_ret)[which(max_daily_ret<=1.00)]
length(selected_symbols_daily)

#subset eod_ret
eod_ret<-eod_ret[,which(colnames(eod_ret) %in% selected_symbols_daily)]
#check
eod_ret[1:10,1:3] #first 10 rows and first 3 columns 
ncol(eod_ret)
nrow(eod_ret)

#YOUR TURN: subset eom_ret data

# Export data from R to CSV -----------------------------------------------
write.csv(eod_ret,'C:/Temp/eod_ret.csv')

# You can actually open this file in Excel!


# Tabular Return Data Analytics -------------------------------------------

RSP<-as.xts(eod_ret[,'SP500TR',drop=F]) #benchmark
RTV<-as.xts(eod_ret[,'TV',drop=F]) 
RTOWN<-as.xts(eod_ret[,'TOWN',drop=F]) 
RTURN<-as.xts(eod_ret[,'TURN',drop=F]) 


tail(RTV)
head(RSP)


# And now we can use the analytical package...

require(PerformanceAnalytics)
#table.Stats(RTV)
#table.Stats(RTOWN)
#table.Stats(RTURN)

# Distributions
#table.Distributions(RTV)
require(PerformanceAnalytics)
#2 question
table.AnnualizedReturns(cbind(RSP,RTV,RTOWN,RTURN),scale=252)

# Returns #1 question
RSP<-as.xts(eod_ret[,'SP500TR',drop=F])
head(RTV)
table.AnnualizedReturns(cbind(RSP),scale=252)

grp_training<-head(cbind(RTV,RTOWN,RTURN),-61)
pt<-cbind(RTV,RTOWN,RTURN)

tail(grp)

chart.CumReturns(cbind(RSP,RTV,RTOWN,RTURN),legend.loc = 'topleft')
chart.CumReturns(cbind(grp),legend.loc = 'topleft')

# Accumulate Returns
acc_RTV<-Return.cumulative(RTV)
acc_RTOWN<-Return.cumulative(RTOWN)
acc_RTURN<-Return.cumulative(RTURN)
acc_RSP<-Return.cumulative(RSP)

cbind(acc_RTV, acc_RTOWN, acc_RTURN, acc_RSP)
# Capital Assets Pricing Model
#table.CAPM(RSP,RTV)

# YOUR TURN: try other tabular analyses

# Graphical Return Data Analytics -----------------------------------------

# Cumulative returns chart
#chart.CumReturns(Ra,legend.loc = 'topleft')
#chart.CumReturns(Rb,legend.loc = 'topleft')

#Box plots
#chart.Boxplot(cbind(Rb,Ra))

#chart.Drawdown(Ra,legend.loc = 'bottomleft')

# YOUR TURN: try other charts

# MV Portfolio Optimization -----------------------------------------------
RSP<-as.xts(eod_ret[,'SP500TR',drop=F]) #benchmark
RTWIN<-as.xts(eod_ret[,'TWIN',drop=F]) 
RTOWN<-as.xts(eod_ret[,'TOWN',drop=F]) 
RTURN<-as.xts(eod_ret[,'TURN',drop=F]) 

RIDXX<-as.xts(eod_ret[,'IDXX',drop=F]) 
RIRMD<-as.xts(eod_ret[,'IRMD',drop=F]) 
RIFJPY<-as.xts(eod_ret[,'IFJPY',drop=F]) 
RPLAY<-as.xts(eod_ret[,'PLAY',drop=F]) 
RPLOW<-as.xts(eod_ret[,'PLOW',drop=F]) 
RPLUS<-as.xts(eod_ret[,'PLUS',drop=F]) 
RPLUG<-as.xts(eod_ret[,'PLUG',drop=F]) 
RPZZA<-as.xts(eod_ret[,'PZZA',drop=F]) 
RPWR<-as.xts(eod_ret[,'PWR',drop=F]) 

pt<-cbind(RIDXX,RIRMD,RIFJPY,RPWR,RPLAY,RPLOW,RPLUS,RPLUG,RPZZA,RTWIN,RTOWN,RTURN)

# withhold the last 253 trading days
#RSP_training<-head(RSP,-253)
#Rb_training<-head(Rb,-253)
PSP_training<-head(RSP,-61)
grp_training<-head(pt,-61)
head(grp_training)
tail(grp_training)
# use the last 253 trading days for testing
grp_testing<-tail(pt,61)
PSP_testing<-tail(RSP,61)
#PSP_testing<-head(PSP_testing,-3)
tail(PSP_testing)
#grp_testing<-head(grp_testing,-3)
tail(grp_testing)
#optimize the MV (Markowitz 1950s) portfolio weights based on training
table.AnnualizedReturns(PSP_training)
mar<-mean(PSP_training) #we need daily minimum acceptable return
mar
require(PortfolioAnalytics)
require(ROI) # make sure to install it
require(ROI.plugin.quadprog)  # make sure to install it
pspec<-portfolio.spec(assets=colnames(grp_training))
pspec<-add.objective(portfolio=pspec,type="risk",name='StdDev')
pspec<-add.constraint(portfolio=pspec,type="full_investment")
pspec<-add.constraint(portfolio=pspec,type="return",return_target=mar)

#optimize portfolio
opt_p<-optimize.portfolio(R=grp_training,portfolio=pspec,optimize_method = 'ROI')

#extract weights (negative weights means shorting)
opt_w<-opt_p$weights
opt_p
opt_w
sum(opt_w)

# YOUR TURN: try adding the long-only constraint and re-optimize the portfolio

#apply weights to test returns
Rp<-PSP_testing # easier to apply the existing structure
#define new column that is the dot product of the two vectors
Rp$ptf<-grp_testing %*% opt_w
Rp$ptf
#check
head(Rp)
tail(Rp)

#Compare basic metrics
table.AnnualizedReturns(Rp)

# Chart Hypothetical Portfolio Returns ------------------------------------
chart.CumReturns(pt,legend.loc = 'bottomright')
chart.CumReturns(Rp,legend.loc = 'bottomright')

pt<-cbind(RIDXX,RIRMD,RIFJPY,RPWR,RPLAY,RPLOW,RPLUS,RPLUG,RPZZA,RTWIN,RTOWN,RTURN)


chart.CumReturns(grp_training,legend.loc = 'topleft')

# End of Part 3c
# End of Stock Market Case Study 
