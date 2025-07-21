DIR=/tmp/.pjk-tests
rm -rf $DIR

pjk '[{up:1,size:3},{up:2,size:4}]' json:$DIR/sinktest # DIR
pjk $DIR/sinktest expect:'[{up:1,size:3},{up:2,size:4}]'

pjk '[{up:1,size:3},{up:2,size:4}]' $DIR/test.json # JSON
pjk $DIR/test.json expect:'[{up:1,size:3},{up:2,size:4}]'

pjk '[{up:1,size:3},{up:2,size:4}]' $DIR/test.json.gz # JSON.GZ
pjk $DIR/test.json.gz expect:'[{up:1,size:3},{up:2,size:4}]'

mv $DIR/test.json $DIR/test.log
pjk $DIR/test.log@format=json expect:'[{up:1,size:3},{up:2,size:4}]'

mv $DIR/test.json.gz $DIR/test.csv
pjk $DIR/test.csv@format=json.gz expect:'[{up:1,size:3},{up:2,size:4}]'

pjk '[{x:2, y:1}, {x:3, y:2}, {x:5, y:7}]' graph:scatter:x:y@pause=1

