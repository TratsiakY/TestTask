from TikTokApi import TikTokApi
import asyncio
from yt_dlp import YoutubeDL
from datetime import datetime
import ffmpeg
import logging
from mdutils.mdutils import MdUtils

'''
Основная задача - реализация пайплайна для получения видео с тиктока и обработки видео соответствующим образом:
1. Уменьшение разрешения видео до 90%
2. уменьшение скорости до 90%
3. замена аудиодорожки
4. сгенерировать небольшой отчет

Имхо наиболее сложная часть - это получить трендовые видео с тиктока. Проблема в том, что сервис постоянно блокирует при попытке подключиться и скачать видео.
Типичный возврат сервера - ниже:
{'log_pb': {'impr_id': '2024062611515309D11640D049F3032659'}, 'statusCode': 10201, 'statusMsg': '', 'status_code': 10201, 'status_msg': ''}
Использование сторонних пакетов для доступа к тиктоку не сильно урощает процесс. Там примерно такая же проблема.

Все реализовано в виде класса Pipeline. 
для доступа к тиктоку- TikTokApi
для скачивания видео был использован YoutubeDL.

Для обработки видео использовал ffmpeg.  


'''

class Pipeline():
    def __init__(self, sfactor=0.9, ffactor=0.9, n = 30, afile = 'song.mp3', report = 'report'):
        '''
        Pipeline class constructor. It contains all required methods that wee need for the test task.
        
        Parameters
        ----------

        sfactor: float
            variable which represent the factor we need to modify the video frame size

        ffactor: float
            variable which represent the factor we need to modify the video frame rate

        n: int
            number of how many video should be downloaded from tiktok

        afile: str
            patch to mp3 audiofile we want to add in all the videos
        '''
        #initialize logging 
        self.logname = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")+'.log'
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename=self.logname, level=logging.INFO)
        self.logger.info('Start logging at ' + datetime.now().strftime("%H : %M : %S"))
        # initialize initial parameters
        self.n = n
        self.sfactor=sfactor
        self.ffactor=ffactor
        self.afile = afile
        self.error = 0
        self.total_time = 0 # total time for pipeline function
        self.mdfile = MdUtils(file_name=report, title='Tiktok video scrapping and processing') # for markdown report


    async def trending_videos(self):
        '''
        Simple method which is used for getting a list of video from the tiktok using TikTok API
        Realized as generator
        '''
        async with TikTokApi() as api:
            await api.create_sessions(headless=False, num_sessions=2, sleep_after=3)
            async for video in api.trending.videos(count=self.n):
                yield video


    async def pipeline(self):
        '''
        The method realizing pipeline of video processing:
        1. request most tranding videos from tiktok
        2. Download video
        3. modification of downloaded video
        '''
        self.total_time = datetime.now()
        self.logger.info('Start pipeline at ' + self.total_time.strftime("%H : %M : %S"))
        counter = 0
        downloaded = set()
        async for video in self.trending_videos():
            user  = video.author.username
            video_id = video.id
            video_url = f"https://www.tiktok.com/@{user}/video/{video_id}"
            video_title = f'{video_id}.mp4' .format(video_id)
            if video_id not in downloaded:
                downloaded.add(video_id)
                res = await self.get_video(video_url)
                if res == 0:
                    vproc = await self.ffmpeg_modif(video_title, self.afile)
                if res == 0 and vproc == 0:
                    counter += 1 #calculate the number of sucesfully processed videos
                

        self.total_time =  datetime.now() - self.total_time
        self.logger.info('End pipeline at ' + datetime.now().strftime("%H : %M : %S"))
        self.report(counter)
        return 0


    async def get_video(self, url):
        '''
        Methos which is using YoutubeDL to download video from tiktok
        '''
        ydl_opts = {'outtmpl': '%(id)s.%(ext)s'} #set naming parameters
        try:
            with YoutubeDL(ydl_opts) as ydl:
                res = ydl.download([url])
                return res
        except:
            self.logger.error(f'Error ossurs during attempt to download video from {url}'.format(url))
            self.error = 1


    async def ffmpeg_modif(self, vfile, afile):
        '''
        Method which uses ffmpeg to modify videos

        Parameters
        ----------

        vfile: str
            Patch to video file
        
        afile: str
            Patch to audio file
        '''
        try:
            #get info about the video (frame shape and fps)
            probe = ffmpeg.probe(vfile)
            video_info = next(s for s in probe['streams'] if s['codec_type'] == 'video')
            fps, sec = video_info['r_frame_rate'].split('/')
            fps = int(int(fps)*self.ffactor/int(sec))
            print(fps)
            width = int(video_info['width'])
            height = int(video_info['height'])

            #modify video
            inp_audio = ffmpeg.input(afile)
            inp_video = (
                ffmpeg
                .input(vfile)
                .filter('scale', int(width*self.sfactor), int(height*self.sfactor))
                .filter('fps', fps = fps)
            )

            (
                ffmpeg
                .output(inp_video, inp_audio, "mod_"+vfile, shortest = None)
                .run()
            )
            return 0
        except:
            self.logger.error(f'Error ossured during attempt to modify {vfile}'.format(vfile))
            self.error = 1
            return 1
        
    def report(self, n):
        '''
        Simple method to create markdown report
        '''
        self.mdfile.new_header(level= 1, title = 'The number of sucesfully processed videos')
        self.mdfile.new_line(str(n))
        self.mdfile.new_header(level= 1, title = 'Total time of scrapping and processing')
        self.mdfile.new_line(str(self.total_time))
        self.mdfile.new_header(level= 1, title = 'Errors during script running')
        if self.error:
            self.mdfile.new_line('See log ' + self.logname)
        else:
            self.mdfile.new_line('None')
        self.mdfile.create_md_file()

           
if __name__ == "__main__":

    asyncio.run(Pipeline().pipeline())


