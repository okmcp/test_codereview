#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#include <camera/NdkCameraDevice.h>
#include <camera/NdkCameraManager.h>
#include <media/NdkImage.h>
#include <media/NdkImageReader.h>

#include "libavformat/avformat.h"
#include "libavformat/internal.h"
#include "libavutil/avstring.h"
#include "libavutil/display.h"
#include "libavutil/imgutils.h"
#include "libavutil/log.h"
#include "libavutil/opt.h"
#include "libavutil/parseutils.h"
#include "libavutil/pixfmt.h"
#include "libavutil/threadmessage.h"
#include "libavutil/time.h"

#include "version.h"
