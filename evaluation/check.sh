
fff=$1
echo "fff=${fff}"
date; egrep INFO ${fff} | tail -1; egrep Write ${fff} | sed -e 's#.*0h0m##' | sed -e 's#s##' -e 's#\.#,#' ; hdfs dfs -du -h /user/chris.arnault | egrep xyz

