
date; egrep INFO nohup.out | tail -1; egrep Write nohup.out | sed -e 's#.*0h0m##' | sed -e 's#s##' -e 's#\.#,#' ; hdfs dfs -du -h /user/chris.arnault | egrep xyz

