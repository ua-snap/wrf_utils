program testread

use netcdf
implicit none

integer :: i,j,k,v,in,mon,day,hr,yr,ftime,itime
integer :: mlen(12),mend,mst
real    :: d2invar(262,262),d2var(262,262,24),d2out(262,262,1)
real    :: d2acc(262,262,24),tmin(262,262,1),tmax(262,262,1)
real    :: missn
character(4) :: year
character(11) :: varlist2d(31),varlist3d(9),acclist2d(5),invar
character(2) :: month,daym,hour
character(255) :: units,lname
character(13) :: fildate
character(10) :: outdate

real, dimension(:,:,:), allocatable :: d3invar
real, dimension(:,:,:,:), allocatable :: d3out
real, dimension(:,:,:,:), allocatable :: d3var

!^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
!Set variables as needed here

varlist2d=(/'ALBEDO     ','CLDFRA     ','CLDFRA_HIGH','CLDFRA_LOW ','CLDFRA_MID ',&
'HFX        ','LH         ','LWDNB      ','LWDNBC     ','LWUPB      ','LWUPBC     ',&
'PSFC       ','Q2         ','QBOT       ','SEAICE     ','SLP        ','SNOW       ',&
'SNOWC      ','SNOWH      ','SWDNB      ','SWDNBC     ','SWUPB      ','SWUPBC     ',&
'T2         ','TBOT       ','TSK        ','U10        ','UBOT       ','V10        ',&
'VBOT       ','VEGFRA     '/)

acclist2d=(/'ACSNOW     ','PCPT       ','PCPC       ','PCPNC      ','POTEVP     '/)

varlist3d=(/'GHT        ','OMEGA      ','QVAPOR     ','SMOIS      ','T          ',&
'TSLB       ','U          ','V          ','SH2O       '/)


mlen=(/31,28,31,30,31,30,31,31,30,31,30,31/)
!print *, '*******warning special dates in code********'
!^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

call getarg(1,year)
read(year,'(i4)') yr
IF (yr < 1950 .or. yr > 2100) stop 'input year not valid'


DO mon=1,12
   mend=mlen(mon)
!  IF (MOD(yr,4) == 0 .and. mon == 2) mend=29
   mst=1
  if (yr == 2005 .and. mon == 1) mst=4 
  DO day=mst,mend

    if (mon < 10) write(month,'("0",i1)') mon
    if (mon > 9) write(month,'(i2)') mon
    if (day < 10) write(daym,'("0",i1)') day
    if (day > 9) write(daym,'(i2)') day

      print *, month,' ',daym

    call makenc(year,month,daym,varlist2d,acclist2d,varlist3d,outdate)
   
!<<<<<<<<>>>>>>>Tmax & Tmin>>>>>>>><<<<<<<<<<
     invar='T2'
     tmax(:,:,:)=-500.0
     tmin(:,:,:)=500.0
    DO hr=0,23
      if (hr < 10) write(hour,'("0",i1)') hr
      if (hr > 9) write(hour,'(i2)') hr
       fildate=year//'-'//month//'-'//daym//'_'//hour
       call read2d(d2invar,invar,fildate,missn,ftime,lname,units) 
       i=46
       j=29   
      DO i=1,262
        DO j=1,262
          IF (tmin(i,j,1) > d2invar(i,j)) tmin(i,j,1) = d2invar(i,j)
          IF (tmax(i,j,1) < d2invar(i,j)) tmax(i,j,1) = d2invar(i,j)
 !         print *,d2invar(i,j),tmin(i,j,1),tmax(i,j,1)
        ENDDO
      ENDDO   
    ENDDO 
     invar='TMIN'
     lname='Daily minimum 2m temperature' 
     call write2d(tmin,missn,outdate,invar,lname,units)
     invar='TMAX'
     lname='Daily maximum 2m temperature' 
     call write2d(tmax,missn,outdate,invar,lname,units)

!<<<<<<<>>>>>>2d Variable Operations>>>>>><<<<<
    DO v=1,31
       invar=varlist2d(v) 
      DO hr=0,23
        if (hr < 10) write(hour,'("0",i1)') hr
        if (hr > 9) write(hour,'(i2)') hr
         fildate=year//'-'//month//'-'//daym//'_'//hour
         call read2d(d2invar,invar,fildate,missn,ftime,lname,units)    
        DO i=1,262
          DO j=1,262
             d2var(i,j,hr+1)=d2invar(i,j)
          ENDDO
        ENDDO   
      ENDDO 
       call avg2d(d2var,d2out,missn)
       call write2d(d2out,missn,outdate,invar,lname,units)
    ENDDO

     ftime=6

!<<<<<<<>>>>>>2d Variable Acc Operations>>>>>><<<<<<
    DO v=1,5
       invar=acclist2d(v)
!       print *, invar
      DO hr=0,23
        if (hr < 10) write(hour,'("0",i1)') hr
        if (hr > 9) write(hour,'(i2)') hr
         fildate=year//'-'//month//'-'//daym//'_'//hour
         call read2d(d2invar,invar,fildate,missn,ftime,lname,units)
        if (hr == 0) itime=ftime
        DO i=1,262
          DO j=1,262
             d2var(i,j,hr+1)=d2invar(i,j)
          ENDDO
        ENDDO       
      ENDDO 
       call accumulator(d2var,d2acc,missn,itime,mon,day,yr,invar)
       call avg2d(d2acc,d2out,missn)
       call write2d(d2out,missn,outdate,invar,lname,units)
!       print *, d2var(15,12)
    ENDDO


!<<<<<<<<>>>>>>3d Variable Operations>>>>>><<<<<
    DO v=1,9
       in=13
      IF (v == 4 .or. v == 6 .or. v == 9) in=4   !change if a soil level variable
       allocate(d3invar(262,262,in))
       allocate(d3out(262,262,in,1))
       allocate(d3var(262,262,in,24))
       invar=varlist3d(v)
      DO hr=0,23
        if (hr < 10) write(hour,'("0",i1)') hr
        if (hr > 9) write(hour,'(i2)') hr
         fildate=year//'-'//month//'-'//daym//'_'//hour   
         call read3d(d3invar,invar,in,fildate,missn,lname,units)
        DO i=1,262
          DO j=1,262
            DO k=1,in
               d3var(i,j,k,hr+1)=d3invar(i,j,k)
            ENDDO
          ENDDO
        ENDDO     
      ENDDO   
       call avg3d(d3var,d3out,in,missn)
       call write3d(d3out,in,missn,outdate,invar,lname,units)
 !      print *, d3var(15,12,1,1),d3out(15,12,1,1),invar
       deallocate(d3var)
       deallocate(d3out)
       deallocate(d3invar)
    ENDDO  

  ENDDO
   mst=1
ENDDO 

end program
!****************************************************************************************
!-------->subroutines below
!----------------------------------------------------------------------------------------
subroutine write3d(d3out,in,missn,outdate,invar,lname,units)

use netcdf
implicit none

integer :: ncid,datid,in
real :: d3out(262,262,in,1),missn

character(255) :: units,lname
character(11) :: invar
character(10) :: outdate

!print *, outdate,invar

call check( NF90_OPEN('WRFDS_d01_'//outdate//'.nc',NF90_WRITE,ncid) )

call check( NF90_INQ_VARID(ncid,TRIM(invar),datid) )

call check( NF90_PUT_VAR(ncid,datid,d3out) )

call check( NF90_REDEF(ncid) )
call check( NF90_PUT_ATT(ncid,datid,'long_name',TRIM(lname)) )
call check( NF90_PUT_ATT(ncid,datid,'units',TRIM(units)) )
call check( NF90_PUT_ATT(ncid,datid,'_FillValue',missn) )
call check( NF90_ENDDEF(ncid) )

call check( NF90_CLOSE(ncid) )

end subroutine write3d
!----------------------------------------------------------------------------------------
subroutine write2d(d2out,missn,outdate,invar,lname,units)

use netcdf
implicit none

integer :: ncid,datid
real :: d2out(262,262,1),missn

character(255) :: units,lname
character(11) :: invar
character(10) :: outdate

!print *, outdate,invar

call check( NF90_OPEN('WRFDS_d01_'//outdate//'.nc',NF90_WRITE,ncid) )

call check( NF90_INQ_VARID(ncid,TRIM(invar),datid) )

call check( NF90_PUT_VAR(ncid,datid,d2out) )

call check( NF90_REDEF(ncid) )
call check( NF90_PUT_ATT(ncid,datid,'long_name',TRIM(lname)) )
call check( NF90_PUT_ATT(ncid,datid,'units',TRIM(units)) )
call check( NF90_PUT_ATT(ncid,datid,'_FillValue',missn) )
call check( NF90_ENDDEF(ncid) )

call check( NF90_CLOSE(ncid) )

end subroutine write2d
!----------------------------------------------------------------------------------------
subroutine makenc(year,month,daym,varlist2d,acclist2d,varlist3d,outdate)

use netcdf
implicit none

integer :: ncid,times(1),i,today(3),ftime
real :: lon(262,262),lat(262,262),rot(262,262),plev(13),sdep1(4),sdep2(4)

integer :: xdimid,ydimid,pdimid,sdimid,timdimid
integer :: latid,lonid,plevid,slevid,rotid,timid,datid

character(11) :: varlist2d(31),varlist3d(9),acclist2d(5)
character(4) :: year
character(2) :: month,daym,hour
character(13) :: fildate
character(10) :: outdate
Character(10) :: fulldate
character(255) :: usern

!>>get info for global attributes
Call idate(today)
Write(fulldate,1002) today(2),today(1),today(3)
1002 FORMAT(i2.2,'/',i2.2,'/',i4)

call GETENV('USER',usern)

!setup date and time information

hour='00'

times=(/0/)

fildate=year//'-'//month//'-'//daym//'_'//hour


!>>>>>>begin file creation<<<<<<<

call getcvars(fildate,lat,lon,rot,plev,sdep1,sdep2,ftime)

outdate=year//'-'//month//'-'//daym

!print *, outdate

call check( NF90_CREATE('WRFDS_d01_'//outdate//'.nc',NF90_CLOBBER,ncid) )

!set coordinates
call check( NF90_DEF_DIM(ncid,'x',262,xdimid) )
call check( NF90_DEF_DIM(ncid,'y',262,ydimid) )
call check( NF90_DEF_DIM(ncid,'lv_press',13,pdimid) )
call check( NF90_DEF_DIM(ncid,'lv_soil',4,sdimid) )
call check( NF90_DEF_DIM(ncid,'time',NF90_UNLIMITED,timdimid) )

!setup coordinate variables
call check( NF90_DEF_VAR(ncid,'time',NF90_INT,timdimid,timid) )
call check( NF90_DEF_VAR(ncid,'plevels',NF90_FLOAT,(/pdimid/),plevid) )
call check( NF90_DEF_VAR(ncid,'slevels',NF90_FLOAT,(/sdimid/),slevid) )
call check( NF90_DEF_VAR(ncid,'lat',NF90_FLOAT,(/xdimid,ydimid/),latid) )
call check( NF90_DEF_VAR(ncid,'lon',NF90_FLOAT,(/xdimid,ydimid/),lonid) )
call check( NF90_DEF_VAR(ncid,'rot',NF90_FLOAT,(/xdimid,ydimid/),rotid) )

!..establish variables to be filled later :::
!2d vars
call check( NF90_DEF_VAR(ncid,'TMAX',NF90_FLOAT,(/xdimid,ydimid,timdimid/),datid) )
call check( NF90_DEF_VAR(ncid,'TMIN',NF90_FLOAT,(/xdimid,ydimid,timdimid/),datid) )

DO i=1,31
   call check( NF90_DEF_VAR(ncid,TRIM(varlist2d(i)),NF90_FLOAT,(/xdimid,ydimid,timdimid/),datid) )
ENDDO
!2d accumulated vars
DO i=1,5
   call check( NF90_DEF_VAR(ncid,TRIM(acclist2d(i)),NF90_FLOAT,(/xdimid,ydimid,timdimid/),datid) )
ENDDO
!3d vars
DO i=1,9
  IF (i == 4 .or. i == 6 .or. i == 9) THEN
     call check( NF90_DEF_VAR(ncid,TRIM(varlist3d(i)),NF90_FLOAT,(/xdimid,ydimid,sdimid,timdimid/),datid) )
  ELSE
     call check( NF90_DEF_VAR(ncid,TRIM(varlist3d(i)),NF90_FLOAT,(/xdimid,ydimid,pdimid,timdimid/),datid) )
  ENDIF   
ENDDO

!>>set coordinate attributes :::
!for pressure levels
call check( NF90_PUT_ATT(ncid,plevid,'units','hPa') )
call check( NF90_PUT_ATT(ncid,plevid,'long_name','isobaric level') )

!for soil levels
call check( NF90_PUT_ATT(ncid,slevid,'units','cm') )
call check( NF90_PUT_ATT(ncid,slevid,'long_name','layer between two depths below land surface') )

!for rot
call check( NF90_PUT_ATT(ncid,rotid,'units','radians') )
call check( NF90_PUT_ATT(ncid,rotid,'long_name','vector rotation angle') )
call check( NF90_PUT_ATT(ncid,rotid,'note1','apply formulas to derive u and v components relative to earth') )
call check( NF90_PUT_ATT(ncid,rotid,'note2','u and v components of vector quantities are resolved relative to grid') )
call check( NF90_PUT_ATT(ncid,rotid,'formula_v','Vearth = cos(rot)*Vgrid - sin(rot)*Ugrid') )
call check( NF90_PUT_ATT(ncid,rotid,'formula_u','Uearth = sin(rot)*Vgrid + cos(rot)*Ugrid') )

!for lat
call check( NF90_PUT_ATT(ncid,latid,'units','degrees_north') )
call check( NF90_PUT_ATT(ncid,latid,'long_name','latitude') )
call check( NF90_PUT_ATT(ncid,latid,'La1',37.233) )
call check( NF90_PUT_ATT(ncid,latid,'Lo1',-177.786) )
call check( NF90_PUT_ATT(ncid,latid,'Lov',-152.) )
call check( NF90_PUT_ATT(ncid,latid,'Dx',19655.) )
call check( NF90_PUT_ATT(ncid,latid,'Dy',19655.) )
call check( NF90_PUT_ATT(ncid,latid,'ProjectionCenter','north') )
call check( NF90_PUT_ATT(ncid,latid,'GridType','Polar Stereographic Projection Grid') )
call check( NF90_PUT_ATT(ncid,latid,'corners',(/37.233,37.236,65.59624,65.58805/)) ) 

!for lon
call check( NF90_PUT_ATT(ncid,lonid,'units','degrees_east') )
call check( NF90_PUT_ATT(ncid,lonid,'long_name','longitude') )
call check( NF90_PUT_ATT(ncid,lonid,'La1',37.233) )
call check( NF90_PUT_ATT(ncid,lonid,'Lo1',-177.786) )
call check( NF90_PUT_ATT(ncid,lonid,'Lov',-152.) )
call check( NF90_PUT_ATT(ncid,lonid,'Dx',19655.) )
call check( NF90_PUT_ATT(ncid,lonid,'Dy',19655.) )
call check( NF90_PUT_ATT(ncid,lonid,'ProjectionCenter','north') )
call check( NF90_PUT_ATT(ncid,lonid,'GridType','Polar Stereographic Projection Grid') )
call check( NF90_PUT_ATT(ncid,lonid,'corners',(/-177.786,-126.2218,-66.01913,122.0177/)) ) 

!for time
call check( NF90_PUT_ATT(ncid,timid,'units','days since '//year//'-'//month//'-'//daym//' 00:00:0.0') )
call check( NF90_PUT_ATT(ncid,timid,'long_name','time') )

!>>set global attributes :::
call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'author',TRIM(usern)) )
call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'project_info','DOI AK CSC dynamical downscaling') )
call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'people','Peter Bieniek, Uma Bhatt, John Walsh, Jing Zhang, Jeremy Krieger, Scott Rupp') )
call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'creation_date',fulldate) )

if (ftime == 6) THEN
   call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'reinit_day','yes') )
else   
   call check( NF90_PUT_ATT(ncid,NF90_GLOBAL,'reinit_day','no') )
endif

call check( NF90_ENDDEF(ncid) )


!write coordinate variables to file
call check( NF90_PUT_VAR(ncid,timid,times) )
call check( NF90_PUT_VAR(ncid,latid,lat) )
call check( NF90_PUT_VAR(ncid,lonid,lon) ) 
call check( NF90_PUT_VAR(ncid,rotid,rot) )
call check( NF90_PUT_VAR(ncid,plevid,plev) )
call check( NF90_PUT_VAR(ncid,slevid,sdep2) )  


call check( NF90_CLOSE(ncid) )

end subroutine makenc
!----------------------------------------------------------------------------------------
subroutine getcvars(fildate,lat,lon,rot,plev,sdep1,sdep2,ftime)

!get the coordinate variables for the netcdf files

use netcdf
implicit none

integer :: ncid,latid,lonid,rotid,plevid,sol1id,sol2id,snid,ftime
real :: lon(262,262),lat(262,262),rot(262,262),plev(13),sdep1(4),sdep2(4)

character(13) :: fildate

call check( NF90_OPEN('WRFDS_d01.'//fildate//'.nc',NF90_NOWRITE,ncid) )
call check( NF90_INQ_VARID(ncid,'g5_lon_1',lonid) )
call check( NF90_INQ_VARID(ncid,'g5_lat_0',latid) )
call check( NF90_INQ_VARID(ncid,'g5_rot_2',rotid) )
call check( NF90_INQ_VARID(ncid,'lv_ISBL2',plevid) )
call check( NF90_INQ_VARID(ncid,'lv_DBLY3_l0',sol1id) )
call check( NF90_INQ_VARID(ncid,'lv_DBLY3_l1',sol2id) )
call check( NF90_INQ_VARID(ncid,'SNOW',snid) )

call check( NF90_GET_ATT(ncid,snid,'forecast_time',ftime) )

call check( NF90_GET_VAR(ncid,lonid,lon) )
call check( NF90_GET_VAR(ncid,latid,lat) )
call check( NF90_GET_VAR(ncid,rotid,rot) )
call check( NF90_GET_VAR(ncid,plevid,plev) )
call check( NF90_GET_VAR(ncid,sol1id,sdep1) )
call check( NF90_GET_VAR(ncid,sol2id,sdep2) )

call check( NF90_CLOSE(ncid) )

end subroutine getcvars

!----------------------------------------------------------------------------------------
subroutine accumulator(d2var,d2acc,missn,ftime,mon,day,yr,invar)

!determine hourly accumulated values for the accumulated variables
!>>Note::WRF accumulation begins with start of each run
!       ->interpolation is required for the first time step

implicit none

integer :: ftime,i,j,t,count,mon,yr,day,mlen(12)
integer :: fmon,fday,fyr,zftime,flag
real    :: d2var(262,262,24),d2acc(262,262,24)
real    :: pdiff,d2invar23(262,262),d2invar22(262,262)
real    :: missn,miss2
character(11) :: invar
character(2) :: month,daym,hour
character(255) :: units,lname
character(4) :: year
character(13) :: fildate

mlen=(/31,28,31,30,31,30,31,31,30,31,30,31/)

fday=day-1
fmon=mon
fyr=yr
IF (fday < 1) THEN
   fmon=fmon-1
  IF (fmon < 1) THEN
     fmon=12
     fyr=fyr-1
  ENDIF
   fday=mlen(fmon)
!  IF (mod(yr,4) == 0 .and. fmon == 2) fday=29     
ENDIF   
   
if (fmon < 10) write(month,'("0",i1)') fmon   
if (fmon >= 10) write(month,'(i2)') fmon
if (fday < 10) write(daym,'("0",i1)') fday
if (fday >= 10) write(daym,'(i2)') fday
write(year,'(i4)') fyr

hour='23'
fildate=year//'-'//month//'-'//daym//'_'//hour
call read2d(d2invar23,invar,fildate,miss2,zftime,lname,units)
hour='22'
fildate=year//'-'//month//'-'//daym//'_'//hour
call read2d(d2invar22,invar,fildate,miss2,zftime,lname,units)

!print *,fmon,fday,fyr,'->',mon,day,yr,invar,ftime
!print *, fildate

flag=0  !set negative precip accumulations to zero
if (invar == 'PCPC' .or. invar == 'PCPNC' .or. invar == 'PCPT' .or. invar == 'ACSNOW') flag = 1

count=0
DO i=1,262
  DO j=1,262
    DO t=2,24
      IF (d2var(i,j,t) /= missn) THEN
         d2acc(i,j,t)=d2var(i,j,t)-d2var(i,j,t-1)
        if (d2acc(i,j,t) < 0.0 .and. flag == 1) then
 !       if (d2var(i,j,t) < 0.0) then
 !          stop '*** accumulation < 0 ***' 
!           print *,t,d2acc(i,j,t),d2var(i,j,t),d2var(i,j,t-1)
           d2acc(i,j,t)=0.0
           count=count+1
        endif   
      ELSE
         d2acc(i,j,t)=missn
      ENDIF
    ENDDO
  ENDDO  
ENDDO
!print *,count,missn,invar,flag


DO i=1,262
  DO j=1,262
    IF (d2var(i,j,1) /= missn .and. d2invar23(i,j) /= miss2) THEN
      IF (ftime /= 6) THEN
         d2acc(i,j,1)=d2var(i,j,1)-d2invar23(i,j)
        if (d2acc(i,j,1) < 0.0 .and. flag == 1) d2acc(i,j,1)=0.0
      ELSE   
         pdiff=d2invar23(i,j)-d2invar22(i,j)
        if (pdiff < 0.0 .and. flag == 1) pdiff=0.0 
         d2acc(i,j,1)=(pdiff+d2acc(i,j,2))/2.0
      ENDIF
    ELSE
       d2acc(i,j,1)=missn     
    ENDIF 
  ENDDO
ENDDO


end subroutine accumulator
!----------------------------------------------------------------------------------------
subroutine avg3d(d3var,d3out,in,missn)

!averages the 3D data

implicit none

integer :: i,j,t,count,in,k
real    :: d3var(262,262,in,24),d3out(262,262,in,1)
real    :: missn,sum

DO i=1,262
  DO j=1,262
    DO k=1,in
       count=0
       sum=0.0
      DO t=1,24
        IF (d3var(i,j,k,t) /= missn) THEN
           count=count+1
           sum=sum+d3var(i,j,k,t)
!           print *, count,sum,d3var(i,j,k,t)
        ENDIF
      ENDDO
      IF (count > 0) THEN
         d3out(i,j,k,1)=sum/real(count)
      ELSE
         d3out(i,j,k,1)=missn
!       print *, missn
      ENDIF
    ENDDO  
  ENDDO  
ENDDO

!print *, d3out(i,j,k,1)

!stop

end subroutine avg3d
!----------------------------------------------------------------------------------------
subroutine avg2d(d2var,d2out,missn)

!averages the 2D data

implicit none

integer :: i,j,t,count
real    :: d2var(262,262,24),d2out(262,262,1)
real    :: missn,sum


DO i=1,262
  DO j=1,262
     count=0
     sum=0.0
    DO t=1,24
      IF (d2var(i,j,t) /= missn) THEN
         count=count+1
         sum=sum+d2var(i,j,t)
 !        print *, t,sum,d2var(i,j,t)
      ENDIF
    ENDDO
    IF (count > 0) THEN
       d2out(i,j,1)=sum/real(count)
 !      print *, d2out(i,j,1)
    ELSE
       d2out(i,j,1)=missn
!       print *, missn
    ENDIF
  ENDDO  
ENDDO



end subroutine avg2d
!----------------------------------------------------------------------------------------
subroutine read3d(d3var,invar,in,fildate,missn,lname,units)

!read 3D data of varying size from netcdf file

use netcdf
implicit none

integer :: ncid,datid,ftime,in
real :: d3var(262,262,in),missn
character(11) :: invar
character(255) :: units,lname
character(13) :: fildate

call check( NF90_OPEN('WRFDS_d01.'//fildate//'.nc',NF90_NOWRITE,ncid) )
call check( NF90_INQ_VARID(ncid,TRIM(invar),datid) )

call check( NF90_GET_VAR(ncid,datid,d3var) )
call check( NF90_GET_ATT(ncid,datid,'forecast_time',ftime) )
call check( NF90_GET_ATT(ncid,datid,'_FillValue',missn) )
call check( NF90_GET_ATT(ncid,datid,'units',units) )
call check( NF90_GET_ATT(ncid,datid,'long_name',lname) )

!print *, invar,ftime,missn,TRIM(units),' ',TRIM(lname)

call check( NF90_CLOSE(ncid) )

end subroutine read3d
!----------------------------------------------------------------------------------------
subroutine read2d(d2var,invar,fildate,missn,ftime,lname,units)

!read 2D data from the netcdf file

use netcdf
implicit none

integer :: ncid,datid,ftime
real :: d2var(262,262),missn
character(11) :: invar
character(255) :: units,lname
character(13) :: fildate

call check( NF90_OPEN('WRFDS_d01.'//fildate//'.nc',NF90_NOWRITE,ncid) )
call check( NF90_INQ_VARID(ncid,TRIM(invar),datid) )

call check( NF90_GET_VAR(ncid,datid,d2var) )
call check( NF90_GET_ATT(ncid,datid,'forecast_time',ftime) )
call check( NF90_GET_ATT(ncid,datid,'_FillValue',missn) )
call check( NF90_GET_ATT(ncid,datid,'units',units) )
call check( NF90_GET_ATT(ncid,datid,'long_name',lname) )

!print *, invar,ftime,missn,TRIM(units),' ',TRIM(lname)

call check( NF90_CLOSE(ncid) )

end subroutine read2d

!----------------------------------------------------------------------------------------
! Check for errors opening netcdf file
  subroutine check(status)
    use netcdf
    integer, intent ( in) :: status
    
    if(status /= nf90_noerr) then 
      print *, nf90_strerror(status)
      stop "Stopped"
    end if
  end subroutine check  
