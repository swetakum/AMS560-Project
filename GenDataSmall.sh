rm -rf Data/Live
mkdir -p Data/Live
for f in Data/Small/x*; do
	echo $f
	cp $f Data/Live/
	sleep 10s
done
