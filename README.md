# scopy

This simple "dd" wrapper was created because "dd conv=noerror,sync" was killing
the bad USB hard drive I was trying to copy the information from - Linux detached
and re-attached the drive (as a new block device) and "dd" failed to do the copy
because of that.
