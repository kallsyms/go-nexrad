#!/bin/sh
source /home/gempak/NAWIPS/Gemenviron.profile

export RAD=$1  # folder to path with l3 files ('NIDS')
export OUT_FN=$2
export PRODUCT=$3
export RADTIM=${4:-current}
export LUT=${5:-GRAY}

export bounds=${bounds:-25.00;-125.00;50.00;-65.00}
export proj=${proj:-mer}
export dims=${dims:-6000;2600}

nex2img << EOF
 GRDAREA  = ${bounds}
 PROJ     = ${proj}
 KXKY     = ${dims}
 CPYFIL   =  
 GFUNC    = ${PRODUCT}
 RADTIM   = ${RADTIM}
 RADDUR   = 15
 RADFRQ   = 
 STNFIL   = nexrad.tbl
 RADMODE  = PC
 RADFIL   = ${OUT_FN}
 LUTFIL   = ${LUT}
 run
 exit
EOF

rm -f gemglb.nts last.nts
