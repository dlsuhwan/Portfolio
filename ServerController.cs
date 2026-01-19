using Google.Apis.AndroidPublisher.v3;
using Google.Apis.AndroidPublisher.v3.Data;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using LibraryRedisClass;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using static LibraryRedisClass.RedisService;

namespace Server.Server.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ServerController : ControllerBase
    {
        private static readonly int ServerVersion = 1;

        private readonly string _logDirectory;
        private readonly string _errorlogDirectory;
        private readonly RedisService _redisService;
        private static readonly Random _random = new Random();

        private static string WhiteColor = "FFFFFF";
        private static string RedColor = "FF0000";
        private static string YellowColor = "E4B421";
        private static string GreenColor = "7EC676";
        private static string[] GradeColor = new string[] { "", "FFFFFF", "5EC5B3", "6A64E1", "DDD667", "E25154" };

        public ServerController(RedisService redisService)
        {
            _redisService = redisService;
            string logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");

            string logDir = Path.Combine(logDirectory, "Log");
            string errorDir = Path.Combine(logDirectory, "Error");

            _logDirectory = logDir;
            _errorlogDirectory = errorDir;

            // 파일에 로그 추가
            if (!Directory.Exists(logDir)) Directory.CreateDirectory(logDir);
            if (!Directory.Exists(errorDir)) Directory.CreateDirectory(errorDir);
        }
        public class GameRequest
        {
            public string StoreType { get; set; } = string.Empty;
            public string Data { get; set; } = string.Empty;
            public int Type { get; set; }
            public string User { get; set; } = string.Empty;
            public int Version { get; set; }
            public string ClientVersion { get; set; } = string.Empty;
            public string Uuid { get; set; } = string.Empty;
        }

        [HttpPost("Pt")]
        public async Task<IActionResult> ProcessPacket([FromForm] GameRequest request)
        {
            try
            {
                var result = await ProcessPacketData(request);
                return Ok(result);
            }
            catch (Exception ex)
            {
                return BadRequest(new { Status = "Error", Message = ex.Message });
            }
        }
        [NonAction]
        public async Task<object> ProcessPacketData(GameRequest request)
        {
            try
            {
                int PacketType = request.Type;
                string UserId = request.User;
                string UUID = request.Uuid;
                string Data = request.Data;
                string StoreType = request.StoreType;

                // 정지 체크
                string banlist = await _redisService.GetBanList(UserId);
                if (banlist == "1")
                {
                    return new
                    {
                        Status = "Userban",
                        Message = "정지된 계정입니다.\n라운지로 문의주세요.",
                    };
                }
                // 점검 체크
                string inspection = await _redisService.GetInspection();
                if (inspection == "1")
                {
                    string whitelist = await _redisService.GetWhiteList(UserId);
                    if (whitelist != "1")
                    {
                        string inspectionNoti = await _redisService.GetInspectionNoti();

                        return new
                        {
                            Status = "Inspection",
                            Message = inspectionNoti,
                        };
                    }
                }

                if (ServerVersion != request.Version)
                {
                    string status = "";
                    string message = "";
                    //클라이언트 서버버전이 더 낮으면 유저는 업데이트를 해야한다.
                    if (request.Version < ServerVersion)
                    {
                        status = "Versionerror";
                        message = "최신 버전으로\n업데이트가 필요합니다.";
                    }
                    //클라이언트 서버버전이 더 높으면 검수로 buildurl로 접속하라고 해야한다.
                    else if (request.Version > ServerVersion)
                    {
                        status = "Buildurl";
                        message = "검수서버로 접속합니다.";

                        WriteLog(UserId, "build server connecting");
                    }

                    return new
                    {
                        Status = status,
                        Message = message,
                    };
                }
                else
                {
                    // 중복 로그인 체크
                    if (PacketType != (int)PACKETTYPE.PT_GET_GAMEDATA && PacketType != (int)PACKETTYPE.PT_Get_USERLOGINLINK)
                    {
                        string UserUUID = await _redisService.GetUserUUID(UserId);

                        if (UserUUID != UUID)
                        {
                            return new
                            {
                                Status = "Duplicatelogin",
                                Message = "중복 로그인으로 인해 종료합니다.",
                            };
                        }
                    }

                    string processdata = await ProcessingPacket(UserId, PacketType, Data, StoreType);

                    return new
                    {
                        Status = "Success",
                        Message = "Data processed successfully",
                        Type = PacketType,
                        ProcessedData = processdata,
                    };
                }
            }
            catch (Exception ex)
            {
                // 에러 로그 파일에 기록
                WriteErrorLog(ex.Message, request);

                return new
                {
                    Status = "Error",
                    Message = ex.Message.ToString(),
                };
            }
        }
        private async Task<string> ProcessingPacket(string UserId, int PacketType, string Data, string StoreType)
        {
            string retunData = "";

            switch (PacketType)
            {
                case (int)PACKETTYPE.PT_Totalpacket: { return await Totalpacket(UserId, Data, StoreType); } break;
            }

            return retunData;
        }

      
        private async Task<string> InitUserData(int PacketType, string UserId, string Data)
        {
            DateTime now = DateTime.Now;
            string[] Data_Int = new string[(int)Characterdata_int.max];
            string[] Data_string = new string[(int)Characterdata_string.max];

            for (int i = 0; i < (int)Characterdata_int.max; i++)
                Data_Int[i] = "0";

            for (int i = 0; i < (int)Characterdata_string.max; i++)
                Data_string[i] = "";

            Data_Int[(int)Characterdata_int.lv] = "1";
            Data_Int[(int)Characterdata_int.heroSlot] = "1";
            Data_Int[(int)Characterdata_int.profileSlot] = "1";
            Data_Int[(int)Characterdata_int.favorSetCount] = "3";
            Data_Int[(int)Characterdata_int.mainQuestIndex] = "1";
            Data_Int[(int)Characterdata_int.mainQuestState] = "1";
            Data_Int[(int)Characterdata_int.tutorialindex] = "1";
            Data_Int[(int)Characterdata_int.FlyingTicket] = "5";
            Data_Int[(int)Characterdata_int.StageCurIndex] = "1";
            Data_Int[(int)Characterdata_int.StageLastIndex] = "1";
            Data_Int[(int)Characterdata_int.MineEnergy] = "20";
            Data_Int[(int)Characterdata_int.MineMaxEnergy] = "20";
            Data_Int[(int)Characterdata_int.MineDigLv] = "1";

            string nickname = await CreateNickname(UserId);
            Data_string[(int)Characterdata_string.nickname] = nickname;
            Data_string[(int)Characterdata_string.createtime] = _redisService.GetDateTimeNow();
            Data_string[(int)Characterdata_string.lastsavetime] = _redisService.GetDateTimeNow();
            Data_string[(int)Characterdata_string.lastFlyingTicketGivenTime] = _redisService.GetDateTimeNow();
            Data_string[(int)Characterdata_string.lastMineEnergyGivenTime] = _redisService.GetDateTimeNow();
            Data_string[(int)Characterdata_string.mineGivenTime] = _redisService.GetDateTimeNow();
            Data_string[(int)Characterdata_string.mineDefense] = "0#0#0#0#0#0#0#0#0#0";
            Data_string[(int)Characterdata_string.mineDefense_Rune] = "0#0#0#0#0#0#0#0#0#0";
            Data_string[(int)Characterdata_string.deviceOption] = "0#0#0#0#0#0#0#0#0#0";
            Data_string[(int)Characterdata_string.bagSlot] = "0#0#0#0#0#0#0#0#0#0";
            Data_string[(int)Characterdata_string.dailyQuestReward] = "0#0#0#0#0";
            Data_string[(int)Characterdata_string.skinSlot] = "0#0#0#0#0";

            string[] newHeroArr = new string[(int)User_Hero.max];
            newHeroArr[(int)User_Hero.index] = "1";
            newHeroArr[(int)User_Hero.count] = "1";
            Data_string[(int)Characterdata_string.inven_hero_data] = string.Join("#", newHeroArr);

            string[] newProfileArr = new string[(int)User_Profile.max];
            newHeroArr[(int)User_Profile.index] = "1";
            newHeroArr[(int)User_Profile.count] = "1";
            Data_string[(int)Characterdata_string.inven_profile_data] = string.Join("#", newHeroArr);

            string[] newAttendanceArr = new string[(int)User_Attendance7days.max];
            newAttendanceArr[(int)User_Attendance7days.lastAttendanceDate] = _redisService.AddDateTimeToday(-1);
            newAttendanceArr[(int)User_Attendance7days.currentDayIndex] = "0";
            newAttendanceArr[(int)User_Attendance7days.rewardDayIndex] = "0#0#0#0#0#0#0";
            Data_string[(int)Characterdata_string.attendance7Days] = string.Join("*", newAttendanceArr);


            await _redisService.SetUserIntData(UserId, String.Join(",", Data_Int));
            await _redisService.SetUserStringData(UserId, String.Join(",", Data_string));

            await _redisService.SetUserNRU(_redisService.GetDateTimeToday(), DateTime.Now.ToString("HH:mm"), UserId);
            await _redisService.SetUserCreateDay(_redisService.GetDateTimeToday(), UserId);


            await _redisService.ProcessPost(UserId, "2", "300", "노옴 환영 보상");
            await _redisService.ProcessPost(UserId, "7", "5", "노옴 환영 보상");
            await _redisService.ProcessPost(UserId, "28", "1", "노옴 환영 보상");

            return await GetUserData(PacketType, UserId, Data);
        }
        private async Task<string> GetUserData(int PacketType, string UserId, string Data)
        {
            string ReturnData = "";

            if (await _redisService.ExistsUserData(UserId) == false)
            {
                return await InitUserData(PacketType, UserId, Data);
            }

            HashEntry[] UserDataInt = await _redisService.HGetAllUserIntData(UserId);
            HashEntry[] UserDatastring = await _redisService.HGetAllUserStringData(UserId);

            Dictionary<int, string> intdata = new Dictionary<int, string>();
            Dictionary<int, string> stringdata = new Dictionary<int, string>();

            for (int i = 0; i < UserDataInt.Length; i++)
            {
                string field = _redisService.RedisToString(UserDataInt[i].Name);
                string value = _redisService.RedisToString(UserDataInt[i].Value);
                intdata.Add(int.Parse(field), value);
            }

            for (int i = 0; i < UserDatastring.Length; i++)
            {
                string field = _redisService.RedisToString(UserDatastring[i].Name);
                string value = _redisService.RedisToString(UserDatastring[i].Value);
                stringdata.Add(int.Parse(field), value);
            }

            // 중간에 데이터 추가시 해당 로직 태워서 추가
            if (stringdata[(int)Characterdata_string.bagSlot].Split('#').Length != 10)
            {
                stringdata[(int)Characterdata_string.bagSlot] = $"{stringdata[(int)Characterdata_string.bagSlot]}#0#0#0#0#0";
            }
            if (intdata.ContainsKey((int)Characterdata_int.MineMaxEnergy) == false || intdata[(int)Characterdata_int.MineMaxEnergy] == "0") 
            {
                intdata[(int)Characterdata_int.MineEnergy] = "20";
                intdata[(int)Characterdata_int.MineMaxEnergy] = "20";
                intdata[(int)Characterdata_int.MineDigLv] = "1";
            
                stringdata[(int)Characterdata_string.lastMineEnergyGivenTime] = _redisService.GetDateTimeNow();
                stringdata[(int)Characterdata_string.mineGivenTime] = _redisService.GetDateTimeNow();
                stringdata[(int)Characterdata_string.mineDefense] = "0#0#0#0#0#0#0#0#0#0";
                stringdata[(int)Characterdata_string.mineDefense_Rune] = "0#0#0#0#0#0#0#0#0#0";
            }
            if (stringdata.ContainsKey((int)Characterdata_string.BeginnerBuff) == false || stringdata[(int)Characterdata_string.BeginnerBuff] == "")
            {
                if (int.Parse(intdata[(int)Characterdata_int.mainQuestIndex]) > 2) 
                {
                    if (int.Parse(intdata[(int)Characterdata_int.lv]) <= 10) stringdata[(int)Characterdata_string.BeginnerBuff] = _redisService.AddDateTimeNow(3);
                    else stringdata[(int)Characterdata_string.BeginnerBuff] = _redisService.GetDateTimeNow();
                }
            }
            if (stringdata.ContainsKey((int)Characterdata_string.skinSlot) == false || stringdata[(int)Characterdata_string.skinSlot] == "")
            {
                stringdata[(int)Characterdata_string.skinSlot] = "0#0#0#0#0";
            }
            if (stringdata.ContainsKey((int)Characterdata_string.EventEndTime) == false || stringdata[(int)Characterdata_string.EventEndTime] == "")
            {
                if (DateTime.Now < DateTime.Parse("2027-02-19"))
                {
                    stringdata[(int)Characterdata_string.EventEndTime] = _redisService.AddDateTimeNow(10);
                    stringdata[(int)Characterdata_string.EventReward] = "0#0#0#0#0#0#0#0#0#0#0#0";
                }
            }

            string userInt = "";
            string userString = "";

            for (int i = 0; i < (int)Characterdata_int.max; i++)
            {
                if (intdata.ContainsKey(i) == true)
                    userInt += intdata[i] + ",";
                else
                    userInt += "0" + ",";
            }

            for (int i = 0; i < (int)Characterdata_string.max; i++)
            {
                if (stringdata.ContainsKey(i) == true)
                    userString += stringdata[i] + ",";
                else
                    userString += "" + ",";
            }

            await _redisService.SetUserIntData(UserId, userInt);
            await _redisService.SetUserStringData(UserId, userString);

            ReturnData = userInt + "|" + userString;

            // Characterdata int, string 외에 따로 관리하는 유저 데이터 체크 없으면 초기화
            await CheckUserData(UserId);

            return ReturnData;
        }
   
        private async Task<string> BillingReward_Shop(string UserId, string templateIndex)
        {
            string ReturnData = "";
            string[] shopDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.shop, templateIndex);

            if (shopDic.Length > 0)
            {
                string name = shopDic[(int)shoptemplate_.name];
                string billingindex = shopDic[(int)shoptemplate_.billingindex];

                string curShopPurchase = await _redisService.GetUserShopPurchase(UserId, templateIndex);
                string[] curShopPurchase_ = curShopPurchase.Split('#');

                bool bFirst = false;
                if (curShopPurchase_[(int)User_shopPurchase.buyCount] == "0")
                {
                    if (templateIndex == "5" || templateIndex == "6" || templateIndex == "7" || templateIndex == "8")
                    {
                        bFirst = true;
                    }
                }

                ReturnData += await ProcessUserShopPurchase(UserId, shopDic);

                // 보상
                string tab = shopDic[(int)shoptemplate_.tab];
                string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                if (rewardtype == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype, "1", true, false);
                else if (rewardtype.Length > 0 && rewardvalue.Length > 0) await _redisService.ProcessPost(UserId, rewardtype, rewardvalue, "결제보상 : " + name);
                
                if (rewardtype2 == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype2, "1", true, false);
                else if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) await _redisService.ProcessPost(UserId, rewardtype2, rewardvalue2, "결제보상 : " + name);

                if (rewardtype3 == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype3, "1", true, false);
                else if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) await _redisService.ProcessPost(UserId, rewardtype3, rewardvalue3, "결제보상 : " + name);

                if (rewardtype4 == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype4, "1", true, false);
                else if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) await _redisService.ProcessPost(UserId, rewardtype4, rewardvalue4, "결제보상 : " + name);

                if (bFirst) 
                {
                    if (rewardtype == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype, "1", true, false);
                    else if (rewardtype.Length > 0 && rewardvalue.Length > 0) await _redisService.ProcessPost(UserId, rewardtype, rewardvalue, "결제보상 : " + name);

                    if (rewardtype2 == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype2, "1", true, false);
                    else if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) await _redisService.ProcessPost(UserId, rewardtype2, rewardvalue2, "결제보상 : " + name);

                    if (rewardtype3 == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype3, "1", true, false);
                    else if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) await _redisService.ProcessPost(UserId, rewardtype3, rewardvalue3, "결제보상 : " + name);

                    if (rewardtype4 == ((int)GoodsType_.Ad_Remove).ToString()) ReturnData += await RewardGoods(UserId, rewardtype4, "1", true, false);
                    else if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) await _redisService.ProcessPost(UserId, rewardtype4, rewardvalue4, "결제보상 : " + name);
                }

                if (tab == ((int)shopTab_.EventShop).ToString()) ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventShopPopup, "");
                else
                { 
                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");
                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_CashShop, "");
                }
            }

            if (templateIndex == "9") ReturnData += MakePacket(ReturnPacket_.ToastDesc, "광고 제거 구매에 성공했습니다.");
            else ReturnData += MakePacket(ReturnPacket_.ToastDesc, "구매에 성공했습니다.\n보상은 우편으로 지급됩니다.");

            return ReturnData;
        }
        private async Task<string> BillingReward_Package(string UserId, string templateIndex)
        {
            string ReturnData = "";
            string[] packageDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.package, templateIndex);

            if (packageDic.Length > 0)
            {
                string type = packageDic[(int)packagetemplate_.type];
                string name = packageDic[(int)packagetemplate_.name];
                string billingindex = packageDic[(int)packagetemplate_.billingindex];

                ReturnData += await ProcessUserPackagePurchase(UserId, packageDic);

                // 보상
                string rewardtype = packageDic[(int)packagetemplate_.rewardtype];
                string rewardvalue = packageDic[(int)packagetemplate_.rewardvalue];
                string rewardtype2 = packageDic[(int)packagetemplate_.rewardtype2];
                string rewardvalue2 = packageDic[(int)packagetemplate_.rewardvalue2];
                string rewardtype3 = packageDic[(int)packagetemplate_.rewardtype3];
                string rewardvalue3 = packageDic[(int)packagetemplate_.rewardvalue3];
                string rewardtype4 = packageDic[(int)packagetemplate_.rewardtype4];
                string rewardvalue4 = packageDic[(int)packagetemplate_.rewardvalue4];
                string rewardtype5 = packageDic[(int)packagetemplate_.rewardtype5];
                string rewardvalue5 = packageDic[(int)packagetemplate_.rewardvalue5];
                string rewardtype6 = packageDic[(int)packagetemplate_.rewardtype6];
                string rewardvalue6 = packageDic[(int)packagetemplate_.rewardvalue6];
                string dailyrewardtype = packageDic[(int)packagetemplate_.dailyrewardtype];
                string dailyrewardvalue = packageDic[(int)packagetemplate_.dailyrewardvalue];

                if (rewardtype.Length > 0 && rewardvalue.Length > 0) await _redisService.ProcessPost(UserId, rewardtype, rewardvalue, "결제보상 : " + name);
                if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) await _redisService.ProcessPost(UserId, rewardtype2, rewardvalue2, "결제보상 : " + name);
                if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) await _redisService.ProcessPost(UserId, rewardtype3, rewardvalue3, "결제보상 : " + name);
                if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) await _redisService.ProcessPost(UserId, rewardtype4, rewardvalue4, "결제보상 : " + name);
                if (rewardtype5.Length > 0 && rewardvalue5.Length > 0) await _redisService.ProcessPost(UserId, rewardtype5, rewardvalue5, "결제보상 : " + name);
                if (rewardtype6.Length > 0 && rewardvalue6.Length > 0) await _redisService.ProcessPost(UserId, rewardtype6, rewardvalue6, "결제보상 : " + name);
                if (dailyrewardtype.Length > 0 && dailyrewardvalue.Length > 0) await _redisService.ProcessPost(UserId, dailyrewardtype, dailyrewardvalue, "결제보상 : " + name);

                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "구매에 성공했습니다.\n보상은 우편으로 지급됩니다.");
                if (type == ((int)packageType_.nomalPacakge).ToString()) ReturnData += MakePacket(ReturnPacket_.Update_Package, templateIndex);
                else if (type == ((int)packageType_.eventPacakge).ToString()) ReturnData += MakePacket(ReturnPacket_.Update_EventPackage, "1");
            }

            return ReturnData;
        }
        private async Task<bool> DailyInit(string UserId)
        {
            string dailyInitDate = await _redisService.GetDailyInitDate(UserId);

            string day = _redisService.GetDateTimeToday();
            if (day == dailyInitDate) return false;

            await _redisService.SetDailyInitDate(UserId, day);
            await _redisService.SetUserDAU(DateTime.Now.ToString("yyyy-MM-dd"));

            return true;
        }
        private async Task<string> CheckdailyInit(string UserId)
        {
            string ReturnData = "";

            bool bdaily = await DailyInit(UserId);

            if (bdaily == true)
            {
                string FlyingTicket = await GetUserData(UserId, Characterdata_int.FlyingTicket);
                if (long.Parse(FlyingTicket) < 5) ReturnData += await SetUserData(UserId, Characterdata_int.FlyingTicket, "5");

                ReturnData += await SetUserData(UserId, Characterdata_int.favorSetCount, "3");
                ReturnData += await SetUserData(UserId, Characterdata_int.dailyQuestPoint, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.FlyingTicket_AdTry, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.FlyingTicket_BuyCount, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.Altar_AdTry, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.PetGachaFree, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.DailyShopResetAdCount, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.DailyShopResetMelaCount, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.MineShopResetAdCount, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.MineShopResetMelaCount, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.AutoMineAdCount, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.MineDefensePlayAdTry, "0");
                ReturnData += await SetUserData(UserId, Characterdata_int.EventPackageTrigger, "0");
                ReturnData += await SetUserData(UserId, Characterdata_string.dailyQuestReward, "0#0#0#0#0");

                ReturnData += await ResetDailyShop(UserId, true);
                ReturnData += await ResetMineShop(UserId, true);
                ReturnData += await ResetEventShop(UserId);
                ReturnData += await CheckRankingReward(UserId);
                await CheckPackageDaily(UserId);

                // dailyQuest init
                string questType = ((int)QuestType_.daily).ToString();

                for (int i = 1; i < (int)QuestIndex_.max; i++)
                {
                    string questIndex = i.ToString();
                    await _redisService.SetQuestCount(UserId, questType, questIndex, "0");
                    await _redisService.SetQuestData(UserId, questType, questIndex, "0");
                }

                await _redisService.SetQuestCount(UserId, ((int)QuestType_.daily).ToString(), ((int)QuestIndex_.Daily_Init).ToString(), "1");

                string createTime = await GetUserData(UserId, Characterdata_string.createtime);
                string createDay = DateTime.Parse(createTime).ToString("yyyy-MM-dd");
                await _redisService.SetUserRetention(createDay);
            }

            return ReturnData;
        }
    
        private async Task<bool> CheckGoods(string UserId, string rewardType, string rewardValue)
        {
            switch (int.Parse(rewardType))
            {
                case (int)GoodsType_.Reward_Profile:
                    {
                        string inven_profile = await GetUserData(UserId, RedisService.Characterdata_string.inven_profile_data);

                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_profile_data = inven_profile.Split('*');

                        for (int i = 0; i < inven_profile_data.Length; i++)
                        {
                            if (inven_profile_data[i] == "") continue;

                            string[] inven_profile_datas = inven_profile_data[i].Split('#');

                            string index = inven_profile_datas[(int)User_Profile.index];
                            string count = inven_profile_datas[(int)User_Profile.count];

                            if (rewardValueDic.ContainsKey(index) == true)
                            {
                                if (rewardValueDic[index] <= int.Parse(count))
                                {
                                    rewardValueDic.Remove(index);
                                }
                                else
                                {
                                    return false;
                                }

                                if (rewardValueDic.Count() <= 0) return true;
                            }
                        }
                    }
                    break;
                case (int)GoodsType_.Reward_Hero:
                    {
                        string inven_hero = await GetUserData(UserId, RedisService.Characterdata_string.inven_hero_data);

                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_hero_data = inven_hero.Split('*');

                        for (int i = 0; i < inven_hero_data.Length; i++)
                        {
                            if (inven_hero_data[i] == "") continue;

                            string[] inven_pet_datas = inven_hero_data[i].Split('#');

                            string index = inven_pet_datas[(int)User_Hero.index];
                            string count = inven_pet_datas[(int)User_Hero.count];

                            if (rewardValueDic.ContainsKey(index) == true)
                            {
                                if (rewardValueDic[index] <= int.Parse(count))
                                {
                                    rewardValueDic.Remove(index);
                                }
                                else
                                {
                                    return false;
                                }

                                if (rewardValueDic.Count() <= 0) return true;
                            }
                        }
                    }
                    break;
                case (int)GoodsType_.Reward_Equip:
                    {
                        string inven_equip = await GetUserData(UserId, RedisService.Characterdata_string.inven_equip_data);

                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_equip_data = inven_equip.Split('*');

                        for (int i = 0; i < inven_equip_data.Length; i++)
                        {
                            if (inven_equip_data[i] == "") continue;

                            string[] inven_equip_datas = inven_equip_data[i].Split('#');

                            string id = inven_equip_datas[(int)User_Equip.id];
                            string count = inven_equip_datas[(int)User_Equip.count];

                            if (rewardValueDic.ContainsKey(id) == true)
                            {
                                if (rewardValueDic[id] <= int.Parse(count))
                                {
                                    rewardValueDic.Remove(id);
                                }
                                else
                                {
                                    return false;
                                }

                                if (rewardValueDic.Count() <= 0) return true;
                            }
                        }

                        return false;
                    }
                    break;
                case (int)GoodsType_.Reward_Skin:
                    {
                        string inven_skin = await GetUserData(UserId, RedisService.Characterdata_string.inven_skin_data);

                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_skin_data = inven_skin.Split('*');

                        for (int i = 0; i < inven_skin_data.Length; i++)
                        {
                            if (inven_skin_data[i] == "") continue;

                            string[] inven_skin_datas = inven_skin_data[i].Split('#');

                            string id = inven_skin_datas[(int)User_Skin.id];
                            string count = inven_skin_datas[(int)User_Skin.count];

                            if (rewardValueDic.ContainsKey(id) == true)
                            {
                                if (rewardValueDic[id] <= int.Parse(count))
                                {
                                    rewardValueDic.Remove(id);
                                }
                                else
                                {
                                    return false;
                                }

                                if (rewardValueDic.Count() <= 0) return true;
                            }
                        }

                        return false;
                    }
                    break;
                case (int)GoodsType_.Reward_Pet:
                    {
                        List<string> rewardValueList = rewardValue.Split('+').ToList();

                        bool bHasPet = true;
                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            string invenIndex = rewardValueList[i];

                            string invenData = await _redisService.GetPetInven(UserId, invenIndex);

                            if (invenData == "") bHasPet = false;
                        }

                        return bHasPet;
                    }
                    break;
                case (int)GoodsType_.Aether:
                    {
                        string inven_aether = await GetUserData(UserId, Characterdata_string.inven_aether_data);

                        List<string> inven_aether_data = inven_aether.Split('*').ToList();

                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        List<string> removeList = new List<string>();

                        bool bHasAether = true;
                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            bool bcheck = false;
                            for (int j = 0; j < inven_aether_data.Count; j++)
                            {
                                string invenIndex = inven_aether_data[j].Split('#')[0];

                                if (rewardValueList.Contains(invenIndex))
                                {
                                    bcheck = true;
                                    break;
                                }
                            }

                            if (bcheck == false) bHasAether = false;
                        }

                        return bHasAether;
                    }
                    break;
                default:
                    {
                        RedisService.Characterdata_int linkint = RedisService.Characterdata_int.none;

                        Dictionary<string, string[]> goodsDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.goods);

                        foreach (var item in goodsDic)
                        {
                            if (item.Value[(int)goodstemplate_.index] == rewardType)
                            {
                                linkint = (RedisService.Characterdata_int)int.Parse(item.Value[(int)goodstemplate_.link_int]);
                                break;
                            }
                        }

                        string curvalue = await GetUserData(UserId, linkint);

                        if (curvalue != "")
                        {
                            if (long.Parse(curvalue) >= long.Parse(rewardValue)) return true;
                            else return false;
                        }
                    }
                    break;
            }

            return false;
        }
        public static string TMPStringColor(string str, string color)
        {
            return $"<color=#{color}>{str}</color>";
        }
        private async Task<string> GetAbilityDesc(string type, string value, string color = "")
        {
            string[] abililtyDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.ability, type);

            string desc = abililtyDic[(int)abilitytemplate_.desc];

            if (color != "")
            {
                desc = desc.Replace("[per]", $"{TMPStringColor("%", color)}");
                desc = desc.Replace("m", $"{TMPStringColor("m", color)}");
                value = TMPStringColor(value, color);
            }
            else
            {
                desc = desc.Replace("[per]", $"{TMPStringColor("%", YellowColor)}");
                desc = desc.Replace("m", $"{TMPStringColor("m", YellowColor)}");
                value = TMPStringColor(value, YellowColor);
            }

            desc = desc.Replace("[value]", value);
            desc = desc.Replace("[per]", "%");

            return desc;
        }
      
        private async Task<string> RewardGoods(string UserId, string rewardType, string rewardValue, bool plus = true, bool popup = true, GachaToastType_ gachaToastType = GachaToastType_.None)
        {
            if (int.Parse(rewardType) < (int)GoodsType_.Reward_Profile && long.Parse(rewardValue) >= 1000000) 
            {
                WriteLog(UserId, $"RewardGoods Over Get Check | rewardType {rewardType} rewardValue {rewardValue}");
            }

            string ReturnData = "";

            switch (int.Parse(rewardType))
            {
                case (int)GoodsType_.Reward_Profile:
                    {
                        string inven_profile = await GetUserData(UserId, Characterdata_string.inven_profile_data);

                        List<string> reddotList = new List<string>();
                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_profile_data = inven_profile.Split('*');

                        for (int i = 0; i < inven_profile_data.Length; i++)
                        {
                            if (inven_profile_data[i] == "") continue;

                            string[] inven_profile_datas = inven_profile_data[i].Split('#');

                            string index = inven_profile_datas[(int)User_Profile.index];
                            string count = inven_profile_datas[(int)User_Profile.count];

                            if (rewardValueDic.ContainsKey(index))
                            {
                                inven_profile_datas[(int)User_Profile.count] = plus ? (int.Parse(count) + rewardValueDic[index]).ToString() : (int.Parse(count) - rewardValueDic[index]).ToString();
                                rewardValueDic.Remove(index);

                                inven_profile_data[i] = string.Join("#", inven_profile_datas);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenProfile, inven_profile_data[i]);
                            }

                            if (rewardValueDic.Count <= 0) break;
                        }

                        inven_profile = string.Join("*", inven_profile_data);

                        foreach (var data in rewardValueDic)
                        {
                            string[] newProfileArr = new string[(int)User_Profile.max];
                            newProfileArr[(int)User_Profile.index] = data.Key;
                            newProfileArr[(int)User_Profile.count] = plus ? data.Value.ToString() : (data.Value * -1).ToString();

                            string newProfile = string.Join("#", newProfileArr);
                            ReturnData += MakePacket(ReturnPacket_.Update_UserInvenProfile, newProfile);

                            if (inven_profile == "") inven_profile = newProfile;
                            else inven_profile += "*" + newProfile;

                            if (plus) reddotList.Add(data.Key);
                        }

                        ReturnData += await SetUserData(UserId, Characterdata_string.inven_profile_data, inven_profile);

                        if (plus)
                        {
                            string curReddot = await GetUserData(UserId, Characterdata_string.reddot_Profile);
                            if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Profile, string.Join("#", reddotList));
                            else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Profile, $"{curReddot}#{string.Join("#", reddotList)}");
                        }
                    }
                    break;
                case (int)GoodsType_.Reward_Hero:
                    {
                        Dictionary<string, string[]> heroDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.hero);

                        string inven_character = await GetUserData(UserId, RedisService.Characterdata_string.inven_hero_data);

                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_character_data = inven_character.Split('*');

                        for (int i = 0; i < inven_character_data.Length; i++)
                        {
                            if (inven_character_data[i] == "") continue;

                            string[] inven_character_datas = inven_character_data[i].Split('#');

                            string index = inven_character_datas[(int)User_Hero.index];
                            string count = inven_character_datas[(int)User_Hero.count];

                            if (rewardValueDic.ContainsKey(index))
                            {
                                inven_character_datas[(int)User_Hero.count] = plus ? (int.Parse(count) + rewardValueDic[index]).ToString() : (int.Parse(count) - rewardValueDic[index]).ToString();
                                rewardValueDic.Remove(index);

                                inven_character_data[i] = string.Join("#", inven_character_datas);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenHero, inven_character_data[i]);
                                if (plus && popup) ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", heroDic[index][(int)herotemplate_.rewardicon], heroDic[index][(int)herotemplate_.name], heroDic[index][(int)herotemplate_.desc]);
                            }

                            if (rewardValueDic.Count <= 0) break;
                        }

                        inven_character = string.Join("*", inven_character_data);

                        foreach (var data in rewardValueDic)
                        {
                            string[] newCharacterArr = new string[(int)User_Hero.max];
                            newCharacterArr[(int)User_Hero.index] = data.Key;
                            newCharacterArr[(int)User_Hero.count] = plus ? data.Value.ToString() : (data.Value * -1).ToString();

                            string newProfile = string.Join("#", newCharacterArr);
                            ReturnData += MakePacket(ReturnPacket_.Update_UserInvenHero, newProfile);

                            if (inven_character == "") inven_character = newProfile;
                            else inven_character += "*" + newProfile;

                            if (plus && popup) ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", heroDic[data.Key][(int)herotemplate_.rewardicon], heroDic[data.Key][(int)herotemplate_.name], heroDic[data.Key][(int)herotemplate_.desc]);
                        }

                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.inven_hero_data, inven_character);
                        if (plus) ReturnData += await OnQuest(UserId, QuestIndex_.Hero_Get, rewardValueList.Count);
                    }
                    break;
                case (int)GoodsType_.Reward_Equip:
                    {
                        Dictionary<string, string[]> equipDic = await _redisService.GetTemplateAll_Indexing(TEMPLATE_TYPE.equip);

                        string inven_equip = await GetUserData(UserId, RedisService.Characterdata_string.inven_equip_data);

                        List<string> reddotList = new List<string>();
                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_equip_data = inven_equip.Split('*');

                        for (int i = 0; i < inven_equip_data.Length; i++)
                        {
                            if (inven_equip_data[i] == "") continue;

                            string[] inven_equip_datas = inven_equip_data[i].Split('#');

                            string id = inven_equip_datas[(int)User_Equip.id];
                            string count = inven_equip_datas[(int)User_Equip.count];

                            if (rewardValueDic.ContainsKey(id))
                            {
                                inven_equip_datas[(int)User_Equip.count] = plus ? (int.Parse(count) + rewardValueDic[id]).ToString() : (int.Parse(count) - rewardValueDic[id]).ToString();
                                rewardValueDic.Remove(id);

                                inven_equip_data[i] = string.Join("#", inven_equip_datas);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenEquip, inven_equip_data[i]);

                                if (plus && popup)
                                {
                                    string grade = equipDic[id][(int)equiptemplate_.grade];
                                    string name = TMPStringColor(equipDic[id][(int)equiptemplate_.name], GradeColor[int.Parse(grade)]);
                                    string abilitytype = equipDic[id][(int)equiptemplate_.abilitytype];
                                    string abilityvalue = equipDic[id][(int)equiptemplate_.abilityvalue];
                                    string desc = await GetAbilityDesc(abilitytype, abilityvalue);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", equipDic[id][(int)equiptemplate_.icon], name, desc, grade);
                                }
                            }

                            if (rewardValueDic.Count <= 0) break;
                        }

                        inven_equip = string.Join("*", inven_equip_data);

                        foreach (var data in rewardValueDic)
                        {
                            string[] newEquipArr = new string[(int)User_Equip.max];
                            newEquipArr[(int)User_Equip.index] = equipDic[data.Key][(int)equiptemplate_.index];
                            newEquipArr[(int)User_Equip.id] = data.Key;
                            newEquipArr[(int)User_Equip.count] = plus ? data.Value.ToString() : (data.Value * -1).ToString();

                            string newProfile = string.Join("#", newEquipArr);
                            ReturnData += MakePacket(ReturnPacket_.Update_UserInvenEquip, newProfile);

                            if (inven_equip == "") inven_equip = newProfile;
                            else inven_equip += "*" + newProfile;

                            if (plus && popup)
                            {
                                string grade = equipDic[data.Key][(int)equiptemplate_.grade];
                                string name = TMPStringColor(equipDic[data.Key][(int)equiptemplate_.name], GradeColor[int.Parse(grade)]);
                                string abilitytype = equipDic[data.Key][(int)equiptemplate_.abilitytype];
                                string abilityvalue = equipDic[data.Key][(int)equiptemplate_.abilityvalue];
                                string desc = await GetAbilityDesc(abilitytype, abilityvalue);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", equipDic[data.Key][(int)equiptemplate_.icon], name, desc, grade);
                                reddotList.Add(data.Key);
                            }
                        }

                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.inven_equip_data, inven_equip);

                        if (plus)
                        {
                            string curReddot = await GetUserData(UserId, Characterdata_string.reddot_Equip);
                            if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Equip, string.Join("#", reddotList));
                            else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Equip, $"{curReddot}#{string.Join("#", reddotList)}");
                        }
                    }
                    break;
                case (int)GoodsType_.Reward_Skin:
                    {
                        Dictionary<string, string[]> skinDic = await _redisService.GetTemplateAll_Indexing(TEMPLATE_TYPE.skin);

                        string inven_skin = await GetUserData(UserId, RedisService.Characterdata_string.inven_skin_data);

                        List<string> reddotList = new List<string>();
                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        for (int i = 0; i < rewardValueList.Count; i++)
                        {
                            if (rewardValueList[i] == "") continue;

                            if (rewardValueDic.ContainsKey(rewardValueList[i])) rewardValueDic[rewardValueList[i]]++;
                            else rewardValueDic.Add(rewardValueList[i], 1);
                        }

                        string[] inven_skin_data = inven_skin.Split('*');

                        for (int i = 0; i < inven_skin_data.Length; i++)
                        {
                            if (inven_skin_data[i] == "") continue;

                            string[] inven_skin_datas = inven_skin_data[i].Split('#');

                            string id = inven_skin_datas[(int)User_Skin.id];
                            string count = inven_skin_datas[(int)User_Skin.count];

                            if (rewardValueDic.ContainsKey(id))
                            {
                                inven_skin_datas[(int)User_Skin.count] = plus ? (int.Parse(count) + rewardValueDic[id]).ToString() : (int.Parse(count) - rewardValueDic[id]).ToString();
                                rewardValueDic.Remove(id);

                                inven_skin_data[i] = string.Join("#", inven_skin_datas);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenSkin, inven_skin_data[i]);

                                if (plus && popup)
                                {
                                    string grade = skinDic[id][(int)skintemplate_.grade];
                                    string name = TMPStringColor(skinDic[id][(int)skintemplate_.name], GradeColor[int.Parse(grade)]);
                                    string abilitytype = skinDic[id][(int)skintemplate_.abilitytype];
                                    string abilityvalue = skinDic[id][(int)skintemplate_.abilityvalue];
                                    string desc = await GetAbilityDesc(abilitytype, abilityvalue);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", skinDic[id][(int)skintemplate_.icon], name, desc, grade);
                                }
                            }

                            if (rewardValueDic.Count <= 0) break;
                        }

                        inven_skin = string.Join("*", inven_skin_data);

                        foreach (var data in rewardValueDic)
                        {
                            string[] newEquipArr = new string[(int)User_Skin.max];
                            newEquipArr[(int)User_Skin.id] = data.Key;
                            newEquipArr[(int)User_Skin.count] = plus ? data.Value.ToString() : (data.Value * -1).ToString();

                            string newProfile = string.Join("#", newEquipArr);
                            ReturnData += MakePacket(ReturnPacket_.Update_UserInvenSkin, newProfile);

                            if (inven_skin == "") inven_skin = newProfile;
                            else inven_skin += "*" + newProfile;

                            if (plus && popup)
                            {
                                string grade = skinDic[data.Key][(int)skintemplate_.grade];
                                string name = TMPStringColor(skinDic[data.Key][(int)skintemplate_.name], GradeColor[int.Parse(grade)]);
                                string abilitytype = skinDic[data.Key][(int)skintemplate_.abilitytype];
                                string abilityvalue = skinDic[data.Key][(int)skintemplate_.abilityvalue];
                                string desc = await GetAbilityDesc(abilitytype, abilityvalue);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", skinDic[data.Key][(int)skintemplate_.icon], name, desc, grade);
                                reddotList.Add(data.Key);
                            }
                        }

                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.inven_skin_data, inven_skin);

                        if (plus)
                        {
                            string curReddot = await GetUserData(UserId, Characterdata_string.reddot_Skin);
                            if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Skin, string.Join("#", reddotList));
                            else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Skin, $"{curReddot}#{string.Join("#", reddotList)}");
                        }
                    }
                    break;
                case (int)GoodsType_.Reward_Pet:
                    {
                        List<string> rewardValueList = rewardValue.Split('+').ToList();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        List<string> reddotList = new List<string>();

                        if (plus)
                        {
                            Dictionary<string, string[]> petDic = await _redisService.GetTemplateAll_Indexing(TEMPLATE_TYPE.pet);

                            for (int i = 0; i < rewardValueList.Count; i++)
                            {
                                string[] newPetArr = new string[(int)User_Pet.max];
                                string invenIndex = await _redisService.GetPetInvenCount(UserId);
                                string id = rewardValueList[i];
                                newPetArr[(int)User_Pet.invenIndex] = invenIndex;
                                newPetArr[(int)User_Pet.index] = petDic[id][(int)pettemplate_.index];
                                newPetArr[(int)User_Pet.id] = petDic[id][(int)pettemplate_.id];
                                newPetArr[(int)User_Pet.type] = petDic[id][(int)pettemplate_.type];
                                newPetArr[(int)User_Pet.grade] = petDic[id][(int)pettemplate_.grade];

                                newPetArr[(int)User_Pet.abilityType] = petDic[id][(int)pettemplate_.abilitytype];

                                float Value_S = float.Parse(petDic[id][(int)pettemplate_.abilityvalue_S]);
                                float Value_E = float.Parse(petDic[id][(int)pettemplate_.abilityvalue_E]);

                                if (Value_S == Value_E) newPetArr[(int)User_Pet.abilityValue] = Value_E.ToString();
                                else newPetArr[(int)User_Pet.abilityValue] = GetRandomFloat(Value_S, Value_E).ToString();

                                string abilityDesc = await GetAbilityDesc(newPetArr[(int)User_Pet.abilityType], newPetArr[(int)User_Pet.abilityValue]);
                                string ability2Desc = "";

                                bool bLuck = false;

                                // 행운
                                if (_random.Next(0, 100) < 3)
                                {
                                    bLuck = true;
                                    newPetArr[(int)User_Pet.abilityType2] = petDic[id][(int)pettemplate_.abilitytype];
                                    newPetArr[(int)User_Pet.abilityValue2] = petDic[id][(int)pettemplate_.addabilityvalue];

                                    ability2Desc = "행운 : " + await GetAbilityDesc(newPetArr[(int)User_Pet.abilityType2], newPetArr[(int)User_Pet.abilityValue2], GreenColor);
                                }

                                string newPet = string.Join("#", newPetArr);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenPet, newPet, invenIndex);

                                await _redisService.SetPetInven(UserId, invenIndex, newPet);
                                await _redisService.SetPetInvenCount(UserId, (long.Parse(invenIndex) + 1).ToString());

                                string abilityIcon = await GetAbilityIcon(newPetArr[(int)User_Pet.abilityType]);
                                string grade = petDic[id][(int)pettemplate_.grade];

                                if (popup)
                                {
                                    string name = TMPStringColor(petDic[id][(int)pettemplate_.name], GradeColor[int.Parse(grade)]);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", petDic[id][(int)pettemplate_.icon], name, abilityDesc, grade, ability2Desc, abilityIcon);
                                }


                                if (int.Parse(grade) > 3) 
                                {
                                    string nickname = await GetUserData(UserId, Characterdata_string.nickname);

                                    string[] gachaToastArr = new string[(int)GachaToast_.max];

                                    for (int j = 0; j < gachaToastArr.Length; j++) gachaToastArr[j] = "";

                                    gachaToastArr[(int)GachaToast_.nickname] = nickname;
                                    gachaToastArr[(int)GachaToast_.gachaType] = ((int)gachaToastType).ToString();
                                    gachaToastArr[(int)GachaToast_.value1] = id;
                                    if(bLuck) gachaToastArr[(int)GachaToast_.value2] = "1";

                                    await _redisService.SetGachaToast(_redisService.GetDateTimeToday(), string.Join("#", gachaToastArr));
                                }

                                reddotList.Add(invenIndex);
                            }

                            string curReddot = await GetUserData(UserId, Characterdata_string.reddot_Pet);
                            if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Pet, string.Join("#", reddotList));
                            else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Pet, $"{curReddot}#{string.Join("#", reddotList)}");
                        }
                        else
                        {
                            // 펫 인벤 지울때는 인벤인덱스로 들어오자
                            for (int i = 0; i < rewardValueList.Count; i++)
                            {
                                string invenIndex = rewardValueList[i];
                                await _redisService.DelPetInven(UserId, invenIndex);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenPet, "", invenIndex);
                            }
                        }
                    }
                    break;
                case (int)GoodsType_.Aether:
                    {
                        string inven_aether = await GetUserData(UserId, Characterdata_string.inven_aether_data);

                        List<string> reddotList = new List<string>();
                        Dictionary<string, int> rewardValueDic = new Dictionary<string, int>();

                        List<string> inven_aether_data = inven_aether.Split('*').ToList();

                        if (plus)
                        {
                            for (int i = 0; i < int.Parse(rewardValue); i++)
                            {
                                string aetherInvenIndex = await GetUserData(UserId, Characterdata_int.AetherInvenIndex);
                                long aetherInvenIndex_ = long.Parse(aetherInvenIndex) + 1;

                                int rand = _random.Next(0, 6);
                                string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.AetherRandomAbility).ToString());
                                string randomAbililty = systemdic[(int)systemtemplate_.value1 + rand];
                                string[] randomAbililtys = randomAbililty.Split('*');

                                string[] newAetherArr = new string[(int)User_Aether.max];
                                for (int j = 0; j < (int)User_Aether.max; j++) newAetherArr[j] = "";

                                newAetherArr[(int)User_Aether.invenIndex] = aetherInvenIndex_.ToString();
                                newAetherArr[(int)User_Aether.abilityType] = randomAbililtys[0];
                                newAetherArr[(int)User_Aether.abilityValue] = randomAbililtys[1];

                                ReturnData += await SetUserData(UserId, Characterdata_int.AetherInvenIndex, aetherInvenIndex_.ToString());
                                reddotList.Add(aetherInvenIndex_.ToString());

                                if (inven_aether == "") inven_aether = string.Join("#", newAetherArr);
                                else inven_aether += "*" + string.Join("#", newAetherArr);
                                
                                string newAether = string.Join("#", newAetherArr);

                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenAether, newAether, aetherInvenIndex_.ToString());

                                if (popup)
                                {
                                    string[] goodsDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.goods, rewardType);
                                    string getWhere = goodsDic[(int)goodstemplate_.where];
                                    string abilityDesc = await GetAbilityDesc(newAetherArr[(int)User_Aether.abilityType], newAetherArr[(int)User_Aether.abilityValue]);

                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", goodsDic[(int)goodstemplate_.icon], goodsDic[(int)goodstemplate_.name], abilityDesc, "", "", "", getWhere);
                                }
                            }
                        }
                        else
                        {
                            List<string> rewardValueList = rewardValue.Split('+').ToList();
                            List<string> removeList = new List<string>();

                            for (int i = 0; i < rewardValueList.Count; i++)
                            {
                                for (int j = 0; j < inven_aether_data.Count; j++)
                                {
                                    string invenIndex = inven_aether_data[j].Split('#')[0];

                                    if (rewardValueList.Contains(invenIndex)) 
                                    {
                                        removeList.Add(inven_aether_data[j]);
                                        break;
                                    }
                                }
                            }

                            for (int i = 0; i < removeList.Count; i++)
                            {
                                if (inven_aether_data.Contains(removeList[i]))
                                { 
                                    string invenIndex = removeList[i].Split('#')[0];
                                    inven_aether_data.Remove(removeList[i]);

                                    ReturnData += MakePacket(ReturnPacket_.Update_UserInvenAether, "", invenIndex);
                                }
                            }
                            inven_aether = string.Join("*", inven_aether_data);
                        }

                        ReturnData += await SetUserData(UserId, Characterdata_string.inven_aether_data, inven_aether);

                        if (plus)
                        {
                            ReturnData += await OnQuest(UserId, RedisService.QuestIndex_.GetAether, long.Parse(rewardValue));

                            string curReddot = await GetUserData(UserId, Characterdata_string.reddot_Aether);
                            if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Aether, string.Join("#", reddotList));
                            else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Aether, $"{curReddot}#{string.Join("#", reddotList)}");
                        }
                    }
                    break;
                case (int)GoodsType_.pet_ticket_4:
                case (int)GoodsType_.pet_ticket_5:
                    {
                        Dictionary<string, string[]> petDic = await _redisService.GetTemplateAll_Indexing(TEMPLATE_TYPE.pet);

                        List<string> rewardList = new List<string>();

                        for (int i = 0; i < int.Parse(rewardValue); i++)
                        {
                            string grade = "";

                            switch (int.Parse(rewardType))
                            {
                                case (int)GoodsType_.pet_ticket_4: grade = "4"; break;
                                case (int)GoodsType_.pet_ticket_5: grade = "5"; break;
                            }

                            string randomBox = await GetRandomBox_Pet_Grade(petDic, grade);
                            if (randomBox != "") rewardList.Add(randomBox);
                        }

                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), string.Join("+", rewardList), true, true, GachaToastType_.PetGacha);
                    }
                    break;
                case (int)GoodsType_.PatternJar:
                case (int)GoodsType_.ShiningJar:
                    {
                        Dictionary<string, string[]> petDic = await _redisService.GetTemplateAll_Indexing(TEMPLATE_TYPE.pet);

                        List<string> rewardList = new List<string>();

                        for (int i = 0; i < int.Parse(rewardValue); i++)
                        {
                            string grade = "";

                            int rand = _random.Next(0, 100); // 0 ~ 99

                            if (int.Parse(rewardType) == (int)GoodsType_.PatternJar)
                            {
                                // 1~4등급
                                if (rand < 30) grade = "1";        // 0 ~ 29
                                else if (rand < 60) grade = "2";   // 30 ~ 59
                                else if (rand < 90) grade = "3";   // 60 ~ 89
                                else grade = "4";                  // 90 ~ 99
                            }
                            else if (int.Parse(rewardType) == (int)GoodsType_.ShiningJar)
                            {
                                // 4~5등급
                                if (rand < 90) grade = "4";        // 0 ~ 89
                                else grade = "5";                  // 90 ~ 99
                            }

                            string randomBox = await GetRandomBox_Pet_Grade(petDic, grade);
                            if (randomBox != "") rewardList.Add(randomBox);
                        }

                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), string.Join("+", rewardList), true, true, GachaToastType_.PetGacha);
                    }
                    break;
                case (int)GoodsType_.RoomBox:
                    {
                        int min = 500;
                        int max = 5000;
                        int step = 100;

                        // (max - min) / step + 1 만큼 경우의 수
                        int count = ((max - min) / step) + 1;

                        int randIndex = _random.Next(0, count); // 0 ~ count-1
                        int roomReward = min + (randIndex * step);
                        
                        WriteLog(UserId, $"open RoomBox - randomIndex : {randIndex}, get room {roomReward}");
                        
                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Room).ToString(), roomReward.ToString());
                    }
                    break;
                case (int)GoodsType_.MelaBox:
                    {
                        int rand = _random.Next(1, 51); // 1 ~ 50

                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Mela).ToString(), rand.ToString());
                    }
                    break;
                case (int)GoodsType_.MaterialsBox:
                    {
                        List<int> ranList = new List<int>();

                        ranList.Add((int)GoodsType_.Material_yellowSend);
                        ranList.Add((int)GoodsType_.Material_RedSend);
                        ranList.Add((int)GoodsType_.Material_BlueSend);
                        ranList.Add((int)GoodsType_.Material_Branch);
                        ranList.Add((int)GoodsType_.Material_Wood);
                        ranList.Add((int)GoodsType_.Material_Reef);
                        ranList.Add((int)GoodsType_.Material_RockPeace);

                        for (int i = 0; i < 8; i++)
                        {
                            for (int j = 0; j < int.Parse(rewardValue); j++)
                            {
                                int randIndex = _random.Next(0, ranList.Count); //

                                ReturnData += await RewardGoods(UserId, ranList[randIndex].ToString(), "1");
                            }
                        }
                    }
                    break;
                case (int)GoodsType_.Ad_Remove:
                    {
                        string utilDate = _redisService.AddDateTimeNow(7);
                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.ad_Remove_Day, utilDate);
                    }
                    break;
                case (int)GoodsType_.BeginnerBuff:
                    {
                        string utilDate = _redisService.AddDateTimeNow(3);
                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.BeginnerBuff, utilDate);
                    }
                    break;
                case (int)GoodsType_.PureMineEnergyPotion:
                    {
                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.MineEnergy).ToString(), "10");
                    }
                    break;
                case (int)GoodsType_.HighPureMineEnergyPotion:
                    {
                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.MineEnergy).ToString(), "20");
                    }
                    break;
                case (int)GoodsType_.Exp:
                    {
                        string[] goodsDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.goods, rewardType);

                        Dictionary<string, string[]> lvDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.lv);

                        string curvalue = await GetUserData(UserId, RedisService.Characterdata_int.exp);
                        long curlv = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.lv));

                        if (plus)
                        {
                            float expIncrease = await _redisService.GetUserAbilityData(UserId, RedisService.Abilitytype_.ExpAmountIncrease);
                            if (expIncrease > 0) rewardValue = (long.Parse(rewardValue) + (long.Parse(rewardValue) * (long)expIncrease) / 100).ToString();
                        }

                        if (curvalue != "")
                        {
                            long setvalue = plus ? (long.Parse(curvalue) + long.Parse(rewardValue)) : (long.Parse(curvalue) - long.Parse(rewardValue));

                            foreach (var data in lvDic.OrderBy(x => int.Parse(x.Key)))
                            {
                                if (long.Parse(data.Value[(int)lvtemplate_.totalexp]) >= setvalue)
                                {
                                    long lv = long.Parse(data.Value[(int)lvtemplate_.lv]);

                                    if (lv > curlv)
                                    {
                                        long addlv = lv - curlv;
                                        string curAbilityPoint = await GetUserData(UserId, RedisService.Characterdata_int.abilityPoint);
                                        curAbilityPoint = (long.Parse(curAbilityPoint) + addlv).ToString();

                                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.abilityPoint, curAbilityPoint);
                                        ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.lv, lv.ToString());
                                    }
                                    break;
                                }
                            }

                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.exp, setvalue.ToString());
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Profile_Lobby, "");

                            if (plus && popup) ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, rewardValue, goodsDic[(int)goodstemplate_.icon], goodsDic[(int)goodstemplate_.name], goodsDic[(int)goodstemplate_.desc]);
                        }
                    }
                    break;
                default:
                    {
                        string[] goodsDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.goods, rewardType);

                        RedisService.Characterdata_int linkint = RedisService.Characterdata_int.none;
                        linkint = (RedisService.Characterdata_int)int.Parse(goodsDic[(int)goodstemplate_.link_int]);

                        string getWhere = goodsDic[(int)goodstemplate_.where];

                        //melaUse
                        if (!plus && linkint == RedisService.Characterdata_int.mela)
                        {
                            ReturnData += await OnQuest(UserId, RedisService.QuestIndex_.Mela_Use, long.Parse(rewardValue));
                        }

                        string curvalue = await GetUserData(UserId, linkint);

                        if (plus && goodsDic[(int)goodstemplate_.subtype] == ((int)GoodsSubType_.Material).ToString()) 
                        {
                            if (curvalue == "0")
                            {
                                string curReddot = await GetUserData(UserId, Characterdata_string.reddot_Material);
                                if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Material, rewardType);
                                else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Material, $"{curReddot}#{rewardType}");
                            }

                            if (linkint == Characterdata_int.Stone_Ore_Magicbullet_Low ||
                                linkint == Characterdata_int.Stone_Ore_Magicbullet_Middle ||
                                linkint == Characterdata_int.Stone_Ore_Magicbullet_High ||
                                linkint == Characterdata_int.Stone_Ore_Barrier_Low ||
                                linkint == Characterdata_int.Stone_Ore_Barrier_Middle ||
                                linkint == Characterdata_int.Stone_Ore_Barrier_High) 
                            {
                                string curReddot = await GetUserData(UserId, Characterdata_string.reddot_MineAmal);
                                string[] curReddots = curReddot.Split('#');

                                if (curReddots.Contains(rewardType) == false) 
                                {
                                    if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_MineAmal, rewardType);
                                    else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_MineAmal, $"{curReddot}#{rewardType}");
                                }
                            }
                        }

                        if (!plus &&
                            (linkint == Characterdata_int.Stone_Ore_Fire ||
                            linkint == Characterdata_int.Stone_Ore_Grass ||
                            linkint == Characterdata_int.Stone_Ore_Water ||
                            linkint == Characterdata_int.Stone_Ore_Dark ||
                            linkint == Characterdata_int.Stone_Ore_Luck)) 
                        {
                            string curReddot = await GetUserData(UserId, Characterdata_string.reddot_MineAmal);
                            string[] curReddots = curReddot.Split('#');

                            if (curReddots.Contains(rewardType) == false)
                            {
                                if (curReddot == "") ReturnData += await SetUserData(UserId, Characterdata_string.reddot_MineAmal, rewardType);
                                else ReturnData += await SetUserData(UserId, Characterdata_string.reddot_MineAmal, $"{curReddot}#{rewardType}");
                            }
                        }

                        if (curvalue != "")
                        {
                            string setvalue = plus ? (long.Parse(curvalue) + long.Parse(rewardValue)).ToString() : (long.Parse(curvalue) - long.Parse(rewardValue)).ToString();
                            ReturnData += await SetUserData(UserId, linkint, setvalue);
                        }

                        if (plus && popup) ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, rewardValue, goodsDic[(int)goodstemplate_.icon], goodsDic[(int)goodstemplate_.name], goodsDic[(int)goodstemplate_.desc], "", "", "", getWhere);
                    }
                    break;
            }

            return ReturnData;
        }
        
        private async Task<string> Totalpacket(string UserId, string Data, string StoreType)
        {
            if (UserId.Length == 0) return "";

            string ReturnData = "";

            using var redLock = await _redisService.UserLock(UserId);
            if (redLock.IsAcquired == false)
            {
                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "잠시 후 다시 시도해주세요.");
                return ReturnData;
            }
            else
            {
                string type = GetData(Data, 0);
                string value1 = GetData(Data, 1);
                string value2 = GetData(Data, 2);
                string value3 = GetData(Data, 3);
                string value4 = GetData(Data, 4);
                string value5 = GetData(Data, 5);
                string value6 = GetData(Data, 6);
                string value7 = GetData(Data, 7);
                string value8 = GetData(Data, 8);
                string value9 = GetData(Data, 9);
                string value10 = GetData(Data, 10);

                int type_ = int.Parse(type);

                switch ((Totalpacket_type)type_)
                {
                    case Totalpacket_type.SetLoginLink:
                        {
                            string curUserId = value1;
                            string firebaseUserId = value2;

                            string checkNick = await GetUserData(firebaseUserId, Characterdata_string.nickname);
                            string linkUserId = await _redisService.getloginlinkid(firebaseUserId);

                            if (checkNick != "" || linkUserId != "")
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 연동된 아이디입니다.");
                                ReturnData += MakePacket(ReturnPacket_.Updateui_OptionPopup, "0");
                            }
                            else
                            {
                                await _redisService.setloginlinkid(firebaseUserId, curUserId);

                                string uuid = await _redisService.GetUserUUID(curUserId);
                                await _redisService.SetUserUUID(firebaseUserId, uuid);

                                ReturnData += MakePacket(ReturnPacket_.ChangeGameID, firebaseUserId);
                                ReturnData += MakePacket(ReturnPacket_.Updateui_OptionPopup, "");

                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "아이디가 연동되었습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.StartServerData:
                        {
                            ReturnData += await CheckdailyInit(UserId);

                            HashEntry[] shopPurchase = await _redisService.GetUserShopPurchase(UserId);

                            for (int i = 0; i < shopPurchase.Length; i++)
                            {
                                string purchaseData = _redisService.RedisToString(shopPurchase[i].Value);

                                ReturnData += MakePacket(ReturnPacket_.Update_ShopPurchase, purchaseData);
                            }

                            HashEntry[] packagePurchase = await _redisService.GetUserPackagePurchase(UserId);

                            for (int i = 0; i < packagePurchase.Length; i++)
                            {
                                string purchaseData = _redisService.RedisToString(packagePurchase[i].Value);

                                ReturnData += MakePacket(ReturnPacket_.Update_PacakgePurchase, purchaseData);
                            }

                            HashEntry[] petInven = await _redisService.GetPetInven(UserId);
                            for (int i = 0; i < petInven.Length; i++)
                            {
                                string petInvenData = _redisService.RedisToString(petInven[i].Value);
                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenPet, petInvenData);
                            }

                            ReturnData += MakePacket(ReturnPacket_.Update_UserInvenTotal, "");

                            ReturnData += await InitQuest(UserId);

                            string tutorialIndex = await GetUserData(UserId, RedisService.Characterdata_int.tutorialindex);
                            if (int.Parse(tutorialIndex) == (int)TutorialIndex_.TryUpFlying ||
                                int.Parse(tutorialIndex) == (int)TutorialIndex_.TryDownFlying ||
                                int.Parse(tutorialIndex) == (int)TutorialIndex_.TryBoosterFlying ||
                                int.Parse(tutorialIndex) == (int)TutorialIndex_.TryShockFlying)
                            {
                                ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.tutorialindex, ((int)TutorialIndex_.StartUnlimitFlying).ToString());
                            }
                            else if (int.Parse(tutorialIndex) == (int)TutorialIndex_.StartSecondDialog ||
                                int.Parse(tutorialIndex) == (int)TutorialIndex_.StartThirdDialog)
                            {
                                ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.mainQuestState, "0");
                            }

                            ReturnData += await CheckRankingReward(UserId);
                            ReturnData += await CheckFlyingTicket(UserId);
                            ReturnData += await CheckMineEnergy(UserId);
                            ReturnData += await CheckMineing(UserId);

                            string nowStr = _redisService.GetDateTimeNow();
                            ReturnData += MakePacket(ReturnPacket_.ServerTimeUpdate, nowStr);

                            //왠만해선 여기 위로, 채팅만 아래                        
                            ReturnData += MakePacket(ReturnPacket_.ServerdataClear, "");

                            string lastChatIndex = await _redisService.GetChatIndex(_redisService.GetDateTimeToday());
                            string lastChatData = await _redisService.GetChatting(_redisService.GetDateTimeToday(), lastChatIndex);

                            if (lastChatData != "")
                            {
                                string[] lastChatDatas = lastChatData.Split('#');
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Chatting, lastChatDatas[0], lastChatDatas[1], lastChatDatas[2]);
                            }

                            ReturnData += MakePacket(ReturnPacket_.Update_ChattingConnect, "");
                        }
                        break;
                    case Totalpacket_type.CheckTime:
                        {
                            string curGachaToastIndex = value1;

                            ReturnData += await CheckdailyInit(UserId);
                            ReturnData += await ManageToast();
                            ReturnData += await CheckPostSchedule(UserId);
                            ReturnData += await CheckGachaToast(curGachaToastIndex);

                            string nowStr = _redisService.GetDateTimeNow();
                            ReturnData += MakePacket(ReturnPacket_.ServerTimeUpdate, nowStr);

                            int currentSecond = DateTime.Now.Second;

                            // 30초 이상 40초 미만일 때 실행할 로직
                            if (currentSecond >= 30 && currentSecond < 40)
                            {
                                // 30초 이상 40초 미만일 때 실행할 로직
                            }

                            ReturnData += MakePacket(ReturnPacket_.Update_ChattingConnect, "");
                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.lastsavetime, DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                            await _redisService.SetUserCCU(DateTime.Now.ToString("yyyy-MM-dd"), DateTime.Now.ToString("HH:mm"), UserId);
                        }
                        break;
                    case Totalpacket_type.Chatting_Get:
                        {
                            string lastChatIndex = await _redisService.GetChatIndex(_redisService.GetDateTimeToday());
                            long lastChatIndex_ = long.Parse(lastChatIndex);

                            if (lastChatIndex_ >= 5)
                            {
                                for (long i = lastChatIndex_ - 5; i <= lastChatIndex_; i++)
                                {
                                    string chatData = await _redisService.GetChatting(_redisService.GetDateTimeToday(), i.ToString());

                                    if (chatData != "")
                                    {
                                        string[] chatDatas = chatData.Split('#');
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Chatting, chatDatas[0], chatDatas[1], chatDatas[2]);
                                    }
                                }
                            }
                            else
                            {
                                for (long i = 1; i <= lastChatIndex_; i++)
                                {
                                    string chatData = await _redisService.GetChatting(_redisService.GetDateTimeToday(), i.ToString());
                                    if (chatData != "")
                                    {
                                        string[] chatDatas = chatData.Split('#');
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Chatting, chatDatas[0], chatDatas[1], chatDatas[2]);
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.InviteChatting_Get:
                        {
                            string lastChatIndex = await _redisService.GetInviteChatIndex(_redisService.GetDateTimeToday());
                            long lastChatIndex_ = long.Parse(lastChatIndex);

                            if (lastChatIndex_ >= 5)
                            {
                                for (long i = lastChatIndex_ - 5; i <= lastChatIndex_; i++)
                                {
                                    string chatData = await _redisService.GetInviteChat(_redisService.GetDateTimeToday(), i.ToString());

                                    if (chatData != "")
                                    {
                                        string[] chatDatas = chatData.Split('#');
                                        ReturnData += MakePacket(ReturnPacket_.Update_InviteChat, chatDatas[0], chatDatas[1], chatDatas[2], chatDatas[3]);
                                    }
                                }
                            }
                            else
                            {
                                for (long i = 1; i <= lastChatIndex_; i++)
                                {
                                    string chatData = await _redisService.GetInviteChat(_redisService.GetDateTimeToday(), i.ToString());
                                    if (chatData != "")
                                    {
                                        string[] chatDatas = chatData.Split('#');
                                        ReturnData += MakePacket(ReturnPacket_.Update_InviteChat, chatDatas[0], chatDatas[1], chatDatas[2], chatDatas[3]);
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.ClearGameReward:
                        {
                            string totalReward = value1;
                            string totalQuest = value2;

                            if (totalReward != "")
                            {
                                string[] totalRewards = totalReward.Split('*');

                                for (int i = 0; i < totalRewards.Length; i++)
                                {
                                    if (totalRewards[i].Length <= 0) continue;

                                    string[] reward = totalRewards[i].Split('#');

                                    if (reward[1] == "0") continue;

                                    if (reward[0] == ((int)GoodsType_.Material_KkaroBag).ToString()) reward[1] = "1";

                                    ReturnData += await RewardGoods(UserId, reward[0], reward[1]);
                                }
                            }

                            if (totalQuest != "")
                            {
                                string[] totalQuests = totalQuest.Split('*');

                                for (int i = 0; i < totalQuests.Length; i++)
                                {
                                    if (totalQuests[i].Length <= 0) continue;

                                    string[] quest = totalQuests[i].Split('#');

                                    int questIndex = int.Parse(quest[0]);
                                    long questCount = long.Parse(quest[1]);

                                    ReturnData += await OnQuest(UserId, (QuestIndex_)questIndex, questCount);
                                }
                            }

                            ReturnData += await SetUserData(UserId, Characterdata_string.altarSet, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_ClearGameToLobby, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                            WriteLog(UserId, $"{(Totalpacket_type)type_} | {totalReward}");
                        }
                        break;
                    case Totalpacket_type.Loby_Open:
                        {
                            ReturnData += await CheckProfile(UserId);
                            ReturnData += await LobbyReddot(UserId);

                            string eventPackageTrigger = await GetUserData(UserId, Characterdata_int.EventPackageTrigger);
                            if (eventPackageTrigger == "0")
                            {
                                string mainQuestIndex = await GetUserData(UserId, Characterdata_int.mainQuestIndex);
                                if (int.Parse(mainQuestIndex) > 3) 
                                {
                                    if (mainQuestIndex == "4") 
                                    {
                                        string mainQuestState = await GetUserData(UserId, Characterdata_int.mainQuestState);
                                        if (mainQuestState == "1" && await CheckGoods(UserId, ((int)GoodsType_.Material_KkaroBag).ToString(), "1") == true) 
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.Update_EventPackage, "0");
                                            ReturnData += await SetUserData(UserId, Characterdata_int.EventPackageTrigger, "1");
                                        }
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.Update_EventPackage, "0");
                                        ReturnData += await SetUserData(UserId, Characterdata_int.EventPackageTrigger, "1");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Post_Open:
                        {
                            HashEntry[] postHash = await _redisService.GetPostAll_hash(UserId);

                            int postcount = 0;

                            for (int i = 0; i < postHash.Length; i++)
                            {
                                string postdata = _redisService.RedisToString(postHash[i].Value);

                                if (postdata == "") continue;

                                string[] postdatas = postdata.Split('#');
                                string index = postdatas[(int)User_PostBox.Index];
                                string endtime = postdatas[(int)User_PostBox.EndTime];

                                if (DateTime.Now > DateTime.Parse(endtime))
                                {
                                    await _redisService.DelPost(UserId, index);
                                    WriteLog(UserId, "del post data overtime" + index + " | " + postdata);
                                    continue;
                                }

                                postcount++;
                                ReturnData += MakePacket(ReturnPacket_.PostAddItem, postdata);
                            }

                            string noticedata = await _redisService.GetNotice();

                            if (noticedata != "")
                            {
                                string[] noticedatas = noticedata.Split('#');

                                for (int i = 0; i < noticedatas.Length; i++)
                                {
                                    if (noticedatas[i] == "") continue;

                                    ReturnData += MakePacket(ReturnPacket_.NoticeAddItem, noticedatas[i]);
                                }
                            }

                            string noticeVersion = await _redisService.GetNoticeVersion();
                            string userNoticeVersion = await GetUserData(UserId, RedisService.Characterdata_int.noticeversion);

                            if (noticeVersion != userNoticeVersion) ReturnData += MakePacket(ReturnPacket_.NoticeRewardOnOff, "1");
                            else ReturnData += MakePacket(ReturnPacket_.NoticeRewardOnOff, "0");
                        }
                        break;
                    case Totalpacket_type.Post_GetItem:
                        {
                            string index = value1;
                            string postitem = await _redisService.GetPost(UserId, index);
                            string[] postdata = postitem.Split('#');

                            await _redisService.DelPost(UserId, index);

                            string rewardtype = postdata[(int)User_PostBox.RewardType];
                            string rewardvalue = postdata[(int)User_PostBox.RewardValue].Replace('_', '#');

                            ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);

                            ReturnData += MakePacket(ReturnPacket_.PostDelItem, index);
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                            WriteLog(UserId, $"{(Totalpacket_type)type_} | rewardtype {(GoodsType_)int.Parse(postdata[(int)User_PostBox.RewardType])} rewardvalue {postdata[(int)User_PostBox.RewardValue]}");
                        }
                        break;
                    case Totalpacket_type.Post_GetItemAll:
                        {
                            HashEntry[] postHash = await _redisService.GetPostAll_hash(UserId);

                            for (int i = 0; i < postHash.Length; i++)
                            {
                                string index = _redisService.RedisToString(postHash[i].Name);
                                string postdatas = _redisService.RedisToString(postHash[i].Value);

                                string[] postdataArr = postdatas.Split('#');

                                string rewardtype = postdataArr[(int)User_PostBox.RewardType];
                                string rewardvalue = postdataArr[(int)User_PostBox.RewardValue].Replace('_', '#');
                                string endtime = postdataArr[(int)User_PostBox.EndTime];

                                ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);

                                await _redisService.DelPost(UserId, index);

                                ReturnData += MakePacket(ReturnPacket_.PostDelItem, index);


                                WriteLog(UserId, $"{(Totalpacket_type)type_} | rewardtype {(GoodsType_)int.Parse(postdataArr[(int)User_PostBox.RewardType])} rewardvalue {postdataArr[(int)User_PostBox.RewardValue]}");
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                        }
                        break;
                    case Totalpacket_type.Notice_GetReward:
                        {
                            string noticeVersion = await _redisService.GetNoticeVersion();

                            string userNoticeVersion = await GetUserData(UserId, RedisService.Characterdata_int.noticeversion);

                            if (noticeVersion != userNoticeVersion)
                            {
                                string[] systemdic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.noticeReward).ToString());

                                string rewardtype = systemdic[(int)systemtemplate_.value1];
                                string rewardvalue = systemdic[(int)systemtemplate_.value2];

                                ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.noticeversion, noticeVersion);

                                ReturnData += MakePacket(ReturnPacket_.NoticeRewardOnOff, "0");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                WriteLog(UserId, $"{(Totalpacket_type)type_}");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 보상을 수령하였습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.Shop_Buy:
                        {
                            string templateIndex = value1;

                            string[] shopDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.shop, templateIndex);

                            if (shopDic.Length > 0)
                            {
                                string tab = shopDic[(int)shoptemplate_.tab];
                                string maxcount = shopDic[(int)shoptemplate_.maxcount];
                                string needtype = shopDic[(int)shoptemplate_.needtype];
                                string needvalue = shopDic[(int)shoptemplate_.needvalue];
                                string billingIndex = shopDic[(int)shoptemplate_.billingindex];

                                string shopPurchase = await _redisService.GetUserShopPurchase(UserId, templateIndex);
                                if (shopPurchase != "")
                                {
                                    string[] shopPurchase_ = shopPurchase.Split("#");

                                    if (DateTime.Parse(shopPurchase_[(int)User_shopPurchase.nextTimingday]) > DateTime.Now)
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 구매한 상품입니다.");
                                    }
                                    else if (int.Parse(shopPurchase_[(int)User_shopPurchase.buyCount]) >= int.Parse(maxcount))
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최대 구매 횟수입니다.");
                                    }
                                    else
                                    {
                                        if (needtype == "0" && billingIndex == "") // 무료 구매 실제 결제가 들어오면 안됌
                                        {
                                            // 유저 상점 인벤데이터 초기화
                                            ReturnData += await ProcessUserShopPurchase(UserId, shopDic);

                                            // 보상
                                            string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                            string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                            string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                            string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                            string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                            string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                            string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                            string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                            if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                            if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                            if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                            if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);


                                            if (tab == ((int)shopTab_.EventShop).ToString()) ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventShopPopup, "");
                                            else ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                        }
                                        else if (await CheckGoods(UserId, needtype, needvalue) == true)
                                        {
                                            // 구매 필요 재화 삭감
                                            ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                            // 유저 상점 인벤데이터 초기화
                                            ReturnData += await ProcessUserShopPurchase(UserId, shopDic);

                                            // 보상
                                            string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                            string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                            string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                            string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                            string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                            string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                            string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                            string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                            if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                            if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                            if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                            if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);

                                            if (tab == ((int)shopTab_.EventShop).ToString()) ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventShopPopup, "");
                                            else ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Profile_PopupOpen:
                        {
                            string favor = await GetUserData(UserId, RedisService.Characterdata_int.favor);

                            ReturnData += MakePacket(ReturnPacket_.CharacterdataInt, ((int)RedisService.Characterdata_int.favor).ToString(), favor);
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                        }
                        break;
                    case Totalpacket_type.Profile_SetIcon:
                        {
                            string profileIndex = value1;

                            string ProfileSlot = await GetUserData(UserId, RedisService.Characterdata_int.profileSlot);

                            if (profileIndex != ProfileSlot)
                            {
                                string inven_profile = await GetUserData(UserId, RedisService.Characterdata_string.inven_profile_data);

                                string[] inven_profile_data = inven_profile.Split('*');

                                bool bChange = false;
                                for (int i = 0; i < inven_profile_data.Length; i++)
                                {
                                    if (inven_profile_data[i] == "") continue;

                                    string[] inven_profile_datas = inven_profile_data[i].Split('#');

                                    string index = inven_profile_datas[(int)User_Profile.index];
                                    string count = inven_profile_datas[(int)User_Profile.count];

                                    if (index == profileIndex && int.Parse(count) > 0)
                                    {
                                        bChange = true;
                                        break;
                                    }
                                }

                                if (bChange)
                                {
                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.profileSlot, profileIndex);

                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Profile_Lobby, "");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "획득하지 않은 사진입니다.");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "사용 중인 사진입니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.Profile_SetNickname:
                        {
                            string nickname = value1;

                            using var totalLock = await _redisService.TotalLock("nickname", nickname);
                            if (totalLock.IsAcquired == false)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다시 시도해주세요.");
                            }
                            else
                            {
                                if(chatBanList.Contains(nickname) || nickname == "관리자" || nickname == "노옴" || nickname == "까로" || nickname == "라부" || nickname == "못")
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "사용할 수 없는 단어가 포함되어있습니다.");
                                }
                                else
                                {
                                    if (await _redisService.CheckNickname(nickname) == false)
                                    {
                                        long nicknameChangeCount = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.nicknameChangeCount));

                                        if (nicknameChangeCount <= 0)
                                        {
                                            await _redisService.SetNickname(nickname, UserId);

                                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.nicknameChangeCount, (nicknameChangeCount + 1).ToString());
                                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.nickname, nickname);
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "닉네임 변경이 완료되었습니다.");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Profile_Lobby, "");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                                        }
                                        else
                                        {
                                            string[] systemdic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.nicknameChange).ToString());

                                            string needtype = systemdic[(int)systemtemplate_.value1];
                                            string needvalue = systemdic[(int)systemtemplate_.value2];

                                            if (await CheckGoods(UserId, needtype, needvalue) == true)
                                            {
                                                ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                                await _redisService.SetNickname(nickname, UserId);

                                                ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.nicknameChangeCount, (nicknameChangeCount + 1).ToString());
                                                ReturnData += await SetUserData(UserId, RedisService.Characterdata_string.nickname, nickname);
                                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "닉네임 변경이 완료되었습니다.");
                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Profile_Lobby, "");
                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                            }
                                            else
                                            {
                                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "중복된 닉네임 입니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Profile_SetUserAbillity:
                        {
                            string useCount = value1;
                            string upgradeData1 = value2;
                            string upgradeData2 = value3;
                            string upgradeData3 = value4;
                            string upgradeData4 = value5;

                            if (useCount != "" && upgradeData1 != "" && upgradeData2 != "" && upgradeData3 != "" && upgradeData4 != "")
                            {
                                Dictionary<string, string[]> characterStatDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.characterstat);

                                int useCount_ = int.Parse(useCount);
                                int upgradeData1_ = int.Parse(upgradeData1);
                                int upgradeData2_ = int.Parse(upgradeData2);
                                int upgradeData3_ = int.Parse(upgradeData3);
                                int upgradeData4_ = int.Parse(upgradeData4);

                                long abailtycount = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.abilityPoint));

                                if (abailtycount >= useCount_ && useCount_ > 0 && useCount_ - (upgradeData1_ + upgradeData2_ + upgradeData3_ + upgradeData4_) == 0)
                                {
                                    if (upgradeData1_ > 0)
                                    {
                                        string basestat = characterStatDic["1"][(int)characterstattemplate_.basestat];
                                        string addstat = characterStatDic["1"][(int)characterstattemplate_.addstat];
                                        string maxstat = characterStatDic["1"][(int)characterstattemplate_.maxstat];

                                        long upgradevalue = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeMoveSpeed));
                                        float trymaxcount = (float.Parse(maxstat) - float.Parse(basestat)) / float.Parse(addstat) - upgradevalue;

                                        if (trymaxcount >= upgradeData1_)
                                        {
                                            upgradevalue = upgradevalue + upgradeData1_;
                                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeMoveSpeed, upgradevalue.ToString());
                                        }
                                    }
                                    if (upgradeData2_ > 0)
                                    {
                                        string basestat = characterStatDic["2"][(int)characterstattemplate_.basestat];
                                        string addstat = characterStatDic["2"][(int)characterstattemplate_.addstat];
                                        string maxstat = characterStatDic["2"][(int)characterstattemplate_.maxstat];

                                        long upgradevalue = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeHp));
                                        float trymaxcount = (float.Parse(maxstat) - float.Parse(basestat)) / float.Parse(addstat) - upgradevalue;

                                        if (trymaxcount >= upgradeData2_)
                                        {
                                            upgradevalue = upgradevalue + upgradeData2_;
                                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeHp, upgradevalue.ToString());
                                        }
                                    }
                                    if (upgradeData3_ > 0)
                                    {
                                        string basestat = characterStatDic["3"][(int)characterstattemplate_.basestat];
                                        string addstat = characterStatDic["3"][(int)characterstattemplate_.addstat];
                                        string maxstat = characterStatDic["3"][(int)characterstattemplate_.maxstat];

                                        long upgradevalue = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeAvoidanceRate));
                                        float trymaxcount = (float.Parse(maxstat) - float.Parse(basestat)) / float.Parse(addstat) - upgradevalue;

                                        if (trymaxcount >= upgradeData3_)
                                        {
                                            upgradevalue = upgradevalue + upgradeData3_;
                                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeAvoidanceRate, upgradevalue.ToString());
                                        }
                                    }
                                    if (upgradeData4_ > 0)
                                    {
                                        string basestat = characterStatDic["4"][(int)characterstattemplate_.basestat];
                                        string addstat = characterStatDic["4"][(int)characterstattemplate_.addstat];
                                        string maxstat = characterStatDic["4"][(int)characterstattemplate_.maxstat];

                                        long upgradevalue = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeLuck));
                                        float trymaxcount = (float.Parse(maxstat) - float.Parse(basestat)) / float.Parse(addstat) - upgradevalue;

                                        if (trymaxcount >= upgradeData4_)
                                        {
                                            upgradevalue = upgradevalue + upgradeData4_;
                                            ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeLuck, upgradevalue.ToString());
                                        }
                                    }

                                    abailtycount = abailtycount - useCount_;
                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.abilityPoint, abailtycount.ToString());

                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "적용되었습니다.");
                                }
                                else
                                {
                                    if (useCount_ == 0)
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "능력치 조정이 되지않았습니다.");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다시 시도해주세요.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Profile_ReSetUserAbillity:
                        {
                            long totalUseCount = 0;
                            long abillityUpgradeValue1 = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeMoveSpeed));
                            long abillityUpgradeValue2 = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeHp));
                            long abillityUpgradeValue3 = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeAvoidanceRate));
                            long abillityUpgradeValue4 = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.upgradeLuck));

                            totalUseCount = abillityUpgradeValue1 + abillityUpgradeValue2 + abillityUpgradeValue3 + abillityUpgradeValue4;

                            if (totalUseCount > 0)
                            {
                                string[] systemdic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.resetUserAbilty).ToString());

                                string needtype = systemdic[(int)systemtemplate_.value1];
                                string needvalue = systemdic[(int)systemtemplate_.value2];

                                if (await CheckGoods(UserId, needtype, needvalue) == true)
                                {
                                    ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                    long abailtycount = long.Parse(await GetUserData(UserId, RedisService.Characterdata_int.abilityPoint));
                                    abailtycount = abailtycount + totalUseCount;

                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeMoveSpeed, "0");
                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeHp, "0");
                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeAvoidanceRate, "0");
                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.upgradeLuck, "0");
                                    ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.abilityPoint, abailtycount.ToString());
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup, "");
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "능력치를 리셋하였습니다.");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "사용한 능력치가 없습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.Profile_SetFavor:
                        {
                            string chatNickname = value1;
                            string setFavorState = value2;

                            string chatUserId = await _redisService.GetUserIDByNick(chatNickname);

                            using var userLock = await _redisService.UserLock(chatUserId);

                            if (userLock.IsAcquired == false)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다시 시도해주세요.");
                            }
                            else
                            {
                                chatNickname = await GetUserData(chatUserId, Characterdata_string.nickname);
                                long chatUserFavor = long.Parse(await GetUserData(chatUserId, Characterdata_int.favor));

                                string myNickname = await GetUserData(UserId, Characterdata_string.nickname);
                                long favorSetCount = long.Parse(await GetUserData(UserId, Characterdata_int.favorSetCount));
                                if (myNickname != chatNickname)
                                {
                                    if (favorSetCount > 0)
                                    {
                                        if (setFavorState == "0")
                                        {
                                            chatUserFavor--;
                                            await OnQuest(chatUserId, QuestIndex_.favor, -1); // 타유저일 떄는 리턴 X
                                        }
                                        else if (setFavorState == "1")
                                        {
                                            chatUserFavor++;
                                            await OnQuest(chatUserId, QuestIndex_.favor, 1); // 타유저일 떄는 리턴 X
                                        }

                                        // 타유저 호감도 수정
                                        await SetUserData(chatUserId, Characterdata_int.favor, chatUserFavor.ToString());

                                        // 내 호감도 세팅 카운트 수정
                                        ReturnData += await SetUserData(UserId, Characterdata_int.favorSetCount, (favorSetCount - 1).ToString());

                                        string[] profileData = new string[(int)profileOther_.max];

                                        profileData[(int)profileOther_.nickname] = await GetUserData(chatUserId, Characterdata_string.nickname);
                                        profileData[(int)profileOther_.profileSlot] = await GetUserData(chatUserId, Characterdata_int.profileSlot);
                                        profileData[(int)profileOther_.favor] = await GetUserData(chatUserId, Characterdata_int.favor);
                                        profileData[(int)profileOther_.upgradeMoveSpeed] = (await _redisService.GetUserAbilityData(chatUserId, Abilitytype_.MoveSpeed)).ToString();
                                        profileData[(int)profileOther_.upgradeHp] = (await _redisService.GetUserAbilityData(chatUserId, Abilitytype_.Hp)).ToString();
                                        profileData[(int)profileOther_.upgradeAvoidanceRate] = (await _redisService.GetUserAbilityData(chatUserId, Abilitytype_.AvoidanceRate)).ToString();
                                        profileData[(int)profileOther_.upgradeLuck] = (await _redisService.GetUserAbilityData(chatUserId, Abilitytype_.Luck)).ToString();

                                        string chatUserBagSlog = await GetUserData(chatUserId, Characterdata_string.bagSlot);
                                        string chatUser_inven_equip = await GetUserData(chatUserId, Characterdata_string.inven_equip_data);
                                        string[] chatUser_equip_data = chatUser_inven_equip.Split('*');
                                       
                                        string[] chatUserBagSlogs = chatUserBagSlog.Split('#');
                                        
                                        for (int i = 0; i < chatUserBagSlogs.Length; i++)
                                        {
                                            if (i == 0 || i == 1)
                                            {
                                                string equipId = chatUserBagSlogs[i];
                                                for (int j = 0; j < chatUser_equip_data.Length; j++)
                                                {
                                                    if (chatUser_equip_data[j] == "") continue;

                                                    string[] chatUser_equip_datas = chatUser_equip_data[j].Split('#');

                                                    if (equipId == chatUser_equip_datas[(int)User_Equip.id])
                                                    {
                                                        string index = chatUser_equip_datas[(int)User_Equip.index];
                                                        string magicAbilityType = chatUser_equip_datas[(int)User_Equip.abilityType];
                                                        string magicAbilityValue = chatUser_equip_datas[(int)User_Equip.abilityValue];

                                                        profileData[(int)profileOther_.bagSlot1 + i] = $"{index}#{magicAbilityType}#{magicAbilityValue}";
                                                        break;
                                                    }
                                                }
                                            }
                                            else if (i == 2 || i == 3 || i == 4)
                                            {
                                                string petInvenData = await _redisService.GetPetInven(chatUserId, chatUserBagSlogs[i]);
                                                if (petInvenData != "")
                                                {
                                                    string[] petInvenDatas = petInvenData.Split('#');
                                                    string index = petInvenDatas[(int)User_Pet.index];
                                                    string abillityType = petInvenDatas[(int)User_Pet.abilityType];
                                                    string abillityValue = petInvenDatas[(int)User_Pet.abilityValue];
                                                    string AddAbillityType = petInvenDatas[(int)User_Pet.abilityType2];
                                                    string AddAbillityValue= petInvenDatas[(int)User_Pet.abilityValue2];

                                                    profileData[(int)profileOther_.bagSlot1 + i] = $"{index}#{abillityType}#{abillityValue}#{AddAbillityType}#{AddAbillityValue}";
                                                }
                                            }
                                        }

                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_ProfilePopup_Other, string.Join("*", profileData));
                                        ReturnData += MakePacket(ReturnPacket_.Update_ManageChat, ((int)ManagerChatType_.Favor).ToString(), setFavorState, chatNickname, myNickname);
                                    
                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | {chatNickname} set {setFavorState}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "오늘 모든 호감도를 사용했습니다. ");
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "자신의 호감도는 설정할 수 없습니다.");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Profile_GetOtherUser:
                        {
                            string chatNickname = value1;
                            string chatUserId = await _redisService.GetUserIDByNick(chatNickname);

                            if (chatUserId != "")
                            {
                                string[] profileData = new string[(int)profileOther_.max];

                                profileData[(int)profileOther_.nickname] = await GetUserData(chatUserId, RedisService.Characterdata_string.nickname);
                                profileData[(int)profileOther_.profileSlot] = await GetUserData(chatUserId, RedisService.Characterdata_int.profileSlot);
                                profileData[(int)profileOther_.favor] = await GetUserData(chatUserId, RedisService.Characterdata_int.favor);
                                profileData[(int)profileOther_.upgradeMoveSpeed] = (await _redisService.GetUserAbilityData(chatUserId, RedisService.Abilitytype_.MoveSpeed)).ToString();
                                profileData[(int)profileOther_.upgradeHp] = (await _redisService.GetUserAbilityData(chatUserId, RedisService.Abilitytype_.Hp)).ToString();
                                profileData[(int)profileOther_.upgradeAvoidanceRate] = (await _redisService.GetUserAbilityData(chatUserId, RedisService.Abilitytype_.AvoidanceRate)).ToString();
                                profileData[(int)profileOther_.upgradeLuck] = (await _redisService.GetUserAbilityData(chatUserId, RedisService.Abilitytype_.Luck)).ToString();

                                string chatUserBagSlog = await GetUserData(chatUserId, Characterdata_string.bagSlot);
                                string chatUser_inven_equip = await GetUserData(chatUserId, Characterdata_string.inven_equip_data);
                                string[] chatUser_equip_data = chatUser_inven_equip.Split('*');

                                string[] chatUserBagSlogs = chatUserBagSlog.Split('#');

                                for (int i = 0; i < chatUserBagSlogs.Length; i++)
                                {
                                    if (i == 0 || i == 1)
                                    {
                                        string equipId = chatUserBagSlogs[i];
                                        for (int j = 0; j < chatUser_equip_data.Length; j++)
                                        {
                                            if (chatUser_equip_data[j] == "") continue;

                                            string[] chatUser_equip_datas = chatUser_equip_data[j].Split('#');

                                            if (equipId == chatUser_equip_datas[(int)User_Equip.id])
                                            {
                                                string index = chatUser_equip_datas[(int)User_Equip.index];
                                                string magicAbilityType = chatUser_equip_datas[(int)User_Equip.abilityType];
                                                string magicAbilityValue = chatUser_equip_datas[(int)User_Equip.abilityValue];

                                                profileData[(int)profileOther_.bagSlot1 + i] = $"{index}#{magicAbilityType}#{magicAbilityValue}";
                                                break;
                                            }
                                        }
                                    }
                                    else if (i == 2 || i == 3 || i == 4)
                                    {
                                        string petInvenData = await _redisService.GetPetInven(chatUserId, chatUserBagSlogs[i]);
                                        if (petInvenData != "")
                                        {
                                            string[] petInvenDatas = petInvenData.Split('#');
                                            string index = petInvenDatas[(int)User_Pet.index];
                                            string abillityType = petInvenDatas[(int)User_Pet.abilityType];
                                            string abillityValue = petInvenDatas[(int)User_Pet.abilityValue];
                                            string AddAbillityType = petInvenDatas[(int)User_Pet.abilityType2];
                                            string AddAbillityValue = petInvenDatas[(int)User_Pet.abilityValue2];

                                            profileData[(int)profileOther_.bagSlot1 + i] = $"{index}#{abillityType}#{abillityValue}#{AddAbillityType}#{AddAbillityValue}";
                                        }
                                    }
                                }

                                ReturnData += MakePacket(ReturnPacket_.OpenUI_ProfilePopup_Other, string.Join("*", profileData));
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "해당 유저의 프로필을 볼 수 없습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.Pet_Gacha:
                        {
                            string gachaCount = value1;

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Pet_Gacha).ToString());

                            string needtype = "";
                            string needvalue = "";

                            bool bFree = false;

                            if (gachaCount == "1")
                            {
                                needtype = systemdic[(int)systemtemplate_.value1];
                                needvalue = systemdic[(int)systemtemplate_.value2];

                                string petGachaFree = await GetUserData(UserId, Characterdata_int.PetGachaFree);
                                if (petGachaFree == "0") bFree = true;
                            }
                            else if (gachaCount == "5")
                            {
                                needtype = systemdic[(int)systemtemplate_.value3];
                                needvalue = systemdic[(int)systemtemplate_.value4];
                            }

                            if (needtype != "" && needvalue != "")
                            {
                                Dictionary<string, string[]> randomtableDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.randomtable);
                                List<string> rewardList = new List<string>();

                                if (bFree)
                                {
                                    string randomBox = GetRandomBox(RandomBoxType_.Pet_Gacha, randomtableDic);
                                    if (randomBox != "") rewardList.Add(randomBox);

                                    string rewardStr = string.Join("+", rewardList);
                                    ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), rewardStr, true, true, GachaToastType_.PetGacha);
                                    ReturnData += await OnQuest(UserId, QuestIndex_.Gacha_Pet, 1);
                                    ReturnData += await SetUserData(UserId, Characterdata_int.PetGachaFree, "1");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_GachaButterfly, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | Free {rewardStr}");
                                }
                                else
                                {
                                    if (await CheckGoods(UserId, needtype, needvalue) == true)
                                    {
                                        ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                        int gachaCount_ = int.Parse(gachaCount);

                                        for (int i = 0; i < gachaCount_; i++)
                                        {
                                            string randomBox = GetRandomBox(RandomBoxType_.Pet_Gacha, randomtableDic);
                                            if (randomBox != "") rewardList.Add(randomBox);
                                        }

                                        string rewardStr = string.Join("+", rewardList);
                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), rewardStr, true, true, GachaToastType_.PetGacha);
                                        ReturnData += await OnQuest(UserId, QuestIndex_.Gacha_Pet, (long)gachaCount_);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_GachaButterfly, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | {rewardStr}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Pet_GachaAniSkip:
                        {
                            string gachaSkip = await GetUserData(UserId, Characterdata_int.butterflyGachaSkip);

                            if (gachaSkip == "0") ReturnData += await SetUserData(UserId, Characterdata_int.butterflyGachaSkip, "1");
                            else ReturnData += await SetUserData(UserId, Characterdata_int.butterflyGachaSkip, "0");

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_PetGachaSkip, "");
                            ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.ui_click).ToString());
                        }
                        break;
                    case Totalpacket_type.Bag_SetEquip:
                        {
                            string equipType = value1;
                            string id = value2;
                            string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                            string[] bagSlot_ = bagSlot.Split('#');

                            int equipType_ = int.Parse(equipType);

                            if (bagSlot_[equipType_] == id)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 장착 중입니다.");
                            }
                            else
                            {
                                bagSlot_[equipType_] = id;
                                bagSlot = string.Join("#", bagSlot_);

                                ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                ReturnData += await OnQuest(UserId, QuestIndex_.Equip_Set, 1);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "0");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_CharacterIcon, "");
                            }
                        }
                        break;
                    case Totalpacket_type.Bag_UnsetEquip:
                        {
                            string equipType = value1;
                            string id = value2;
                            string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                            string[] bagSlot_ = bagSlot.Split('#');

                            int equipType_ = int.Parse(equipType);

                            if (bagSlot_[equipType_] == id)
                            {
                                bagSlot_[equipType_] = "0";
                                bagSlot = string.Join("#", bagSlot_);

                                ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "0");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_CharacterIcon, "");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다시 시도해주세요.");
                            }
                        }
                        break;
                    case Totalpacket_type.Bag_SetPet:
                        {
                            string invenIndex = value1;
                            string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                            string[] bagSlot_ = bagSlot.Split('#');

                            if (bagSlot_[2] == invenIndex || bagSlot_[3] == invenIndex || bagSlot_[4] == invenIndex)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 장착 중인 나비입니다.");
                            }
                            else
                            {
                                int setIndex = 0;

                                string invenData = await _redisService.GetPetInven(UserId, invenIndex);
                                bool bCheage = false;
                                if (invenData != "")
                                {
                                    string[] invenDatas = invenData.Split('#');
                                    string petType = invenDatas[(int)User_Pet.type];

                                    for (int i = 2; i <= 4; i++)
                                    {
                                        if (bagSlot_[i] != "0")
                                        {
                                            string slotInvenData = await _redisService.GetPetInven(UserId, bagSlot_[i]);

                                            if (slotInvenData != "")
                                            {
                                                string[] slotInvenDatas = slotInvenData.Split('#');

                                                // 기존 착용이 같은 종료 타입 나비면
                                                if (petType == slotInvenDatas[(int)User_Pet.type])
                                                {
                                                    bCheage = true;
                                                    bagSlot_[i] = invenIndex;
                                                    bagSlot = string.Join("#", bagSlot_);

                                                    ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "1");
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (bCheage == false)
                                {
                                    if (bagSlot_[2] == "0") setIndex = 2;
                                    else if (bagSlot_[3] == "0") setIndex = 3;
                                    else if (bagSlot_[4] == "0") setIndex = 4;


                                    if (setIndex > 0)
                                    {
                                        bagSlot_[setIndex] = invenIndex;
                                        bagSlot = string.Join("#", bagSlot_);

                                        ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "1");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "모든 나비가 장착 중입니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Bag_UnsetPet:
                        {
                            string invenIndex = value1;
                            string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                            string[] bagSlot_ = bagSlot.Split('#');

                            int setIndex = 0;

                            if (bagSlot_[2] == invenIndex) setIndex = 2;
                            else if (bagSlot_[3] == invenIndex) setIndex = 3;
                            else if (bagSlot_[4] == invenIndex) setIndex = 4;

                            if (setIndex > 0)
                            {
                                bagSlot_[setIndex] = "0";
                                bagSlot = string.Join("#", bagSlot_);

                                ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "1");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다시 시도해주세요.");
                            }
                        }
                        break;
                    case Totalpacket_type.Option_DeviceSet:
                        {
                            string optionType = value1;
                            string optionValue = value2;

                            string[] deviceOptcion = (await GetUserData(UserId, Characterdata_string.deviceOption)).Split('#');
                            if (deviceOptcion[int.Parse(optionType)] != optionValue)
                            {
                                deviceOptcion[int.Parse(optionType)] = optionValue;
                                ReturnData += await SetUserData(UserId, Characterdata_string.deviceOption, string.Join("#", deviceOptcion));
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Option_Device, optionType, optionValue);
                                ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.ui_click).ToString());
                            }
                        }
                        break;
                    case Totalpacket_type.Option_CouponUse:
                        {
                            string couponName = value1;

                            string couponDetail = await _redisService.GetCoupon(couponName);
                            string[] couponDetails = couponDetail.Split('#');

                            if (couponDetail == "")
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "등록되지 않은 쿠폰입니다.");
                            }
                            else
                            {
                                string reward = couponDetails[(int)Coupon_.reward];
                                string couponGroup = couponDetails[(int)Coupon_.group];
                                string couponType = couponDetails[(int)Coupon_.type];
                                string couponUseMaxCount = couponDetails[(int)Coupon_.useMaxCount];
                                string couponStartTime = couponDetails[(int)Coupon_.startTime];
                                string couponEndTime = couponDetails[(int)Coupon_.endTime];

                                // 쿠폰 사용 체크
                                string couponUse = await _redisService.GetCouponUse(couponName);
                                List<string> couponUseList = couponUse == "" ? new List<string>() : couponUse.Split('#').ToList();

                                string couponGroupUse = await _redisService.GetCouponGroupUse(couponGroup);
                                List<string> couponGroupUseList = couponGroupUse == "" ? new List<string>() : couponGroupUse.Split('#').ToList();

                                if (couponUseList.Contains(UserId))
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "사용한 쿠폰입니다.");
                                }
                                else
                                {
                                    bool bUseAble = false;
                                    switch (int.Parse(couponType))
                                    {
                                        case (int)CouponType_.Public:
                                            {
                                                bUseAble = true;
                                            }
                                            break;
                                        case (int)CouponType_.Group:
                                            {
                                                // 그룹 쿠폰 사용 체크
                                                if (couponGroupUse.Contains(UserId)) ReturnData += MakePacket(ReturnPacket_.ToastDesc, "사용한 그룹 쿠폰입니다.");
                                                else bUseAble = true;
                                            }
                                            break;
                                        case (int)CouponType_.Personal:
                                            {
                                                // 쿠폰 사용 개수 체크
                                                if (couponUseList.Count >= int.Parse(couponUseMaxCount)) ReturnData += MakePacket(ReturnPacket_.ToastDesc, "소진된 쿠폰입니다.");
                                                else bUseAble = true;
                                            }
                                            break;
                                    }

                                    if (bUseAble)
                                    {
                                        if (DateTime.Now >= DateTime.Parse(couponStartTime) && DateTime.Now < DateTime.Parse(couponEndTime))
                                        {
                                            // 쿠폰 지급
                                            string[] rewards = reward.Split('^');
                                            for (int i = 0; i < rewards.Length; i++)
                                            {
                                                string[] rewards_ = rewards[i].Split('*');
                                                string rewardType = rewards_[0];
                                                string rewardValue = rewards_[1];

                                                await _redisService.ProcessPost(UserId, rewardType, rewardValue, $"{couponName} 쿠폰 보상");
                                            }

                                            // 사용자 추가
                                            couponUseList.Add(UserId);
                                            await _redisService.SetCouponUse(couponName, string.Join("#", couponUseList));

                                            if (int.Parse(couponType) == (int)CouponType_.Group)
                                            {
                                                couponGroupUseList.Add(UserId);
                                                await _redisService.SetCouponGroupUse(couponGroup, string.Join("#", couponGroupUseList));
                                            }

                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "쿠폰 보상이 우편지급되었습니다.");

                                            WriteLog(UserId, $"{(Totalpacket_type)type_} | {couponName}");
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Transmute_Make_Equip:
                        {
                            string index = value1;

                            string[] equipDic = await _redisService.GetTemplate(TEMPLATE_TYPE.equip, index);

                            string id = equipDic[(int)equiptemplate_.id];
                            string equipType = equipDic[(int)equiptemplate_.type];

                            string inven_equip = await GetUserData(UserId, Characterdata_string.inven_equip_data);
                            string[] inven_equip_data = inven_equip.Split('*');

                            bool bHas = false;
                            for (int i = 0; i < inven_equip_data.Length; i++)
                            {
                                if (inven_equip_data[i] == "") continue;

                                string[] inven_equip_datas = inven_equip_data[i].Split('#');

                                if (id == inven_equip_datas[(int)User_Equip.id])
                                {
                                    if (int.Parse(inven_equip_datas[(int)User_Equip.count]) > 0)
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 제작되었습니다.");
                                        bHas = true;
                                        break;
                                    }
                                }
                            }

                            if (bHas == false)
                            {
                                bool bCanMake = true;
                                for (int i = (int)equiptemplate_.needtype; i <= (int)equiptemplate_.needvalue5; i = i + 2)
                                {
                                    string needType = equipDic[i];
                                    string needValue = equipDic[i + 1];

                                    if (needType != "" && needValue != "")
                                    {
                                        if (await CheckGoods(UserId, needType, needValue) == false)
                                        {
                                            bCanMake = false;
                                            break;
                                        }
                                    }
                                }

                                if (bCanMake == false)
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                }
                                else
                                {
                                    for (int i = (int)equiptemplate_.needtype; i <= (int)equiptemplate_.needvalue5; i = i + 2)
                                    {
                                        string needType = equipDic[i];
                                        string needValue = equipDic[i + 1];

                                        if (needType != "" && needValue != "")
                                        {
                                            ReturnData += await RewardGoods(UserId, needType, needValue, false);
                                        }
                                    }

                                    ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Equip).ToString(), id);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Transmute_Tab, equipType);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Transmute_Equip_Detail, index);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | {index}");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Transmute_AddMagic_Equip:
                        {
                            string index = value1;

                            string[] equipDic = await _redisService.GetTemplate(TEMPLATE_TYPE.equip, index);

                            string id = equipDic[(int)equiptemplate_.id];
                            string equipType = equipDic[(int)equiptemplate_.type];

                            string inven_equip = await GetUserData(UserId, Characterdata_string.inven_equip_data);
                            string[] inven_equip_data = inven_equip.Split('*');

                            bool bHas = false;
                            int invenIndex = -1;
                            for (int i = 0; i < inven_equip_data.Length; i++)
                            {
                                if (inven_equip_data[i] == "") continue;

                                string[] inven_equip_datas = inven_equip_data[i].Split('#');

                                if (id == inven_equip_datas[(int)User_Equip.id])
                                {
                                    invenIndex = i;
                                    if (int.Parse(inven_equip_datas[(int)User_Equip.count]) > 0)
                                    {
                                        bHas = true;
                                        break;
                                    }
                                }
                            }

                            if (bHas == false)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "제작이 필요합니다.");
                            }
                            else
                            {
                                if (await CheckGoods(UserId, ((int)GoodsType_.Material_MagicStone).ToString(), "1") == false)
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                }
                                else
                                {
                                    if (invenIndex >= 0)
                                    {
                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Material_MagicStone).ToString(), "1", false);

                                        string[] inven_equip_datas = inven_equip_data[invenIndex].Split('#');

                                        string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Transmute_RandomAbility).ToString());

                                        string[] ranAbilityType = systemdic[(int)systemtemplate_.value1].Split('#');
                                        string[] ranAbilityValue = systemdic[(int)systemtemplate_.value2].Split('*');
                                        int ranIndex = _random.Next(0, ranAbilityType.Length);

                                        string[] ranAbilityValues = ranAbilityValue[ranIndex].Split('#');
                                        int ranValue = _random.Next(int.Parse(ranAbilityValues[0]), int.Parse(ranAbilityValues[1]) + 1);

                                        inven_equip_datas[(int)User_Equip.abilityType] = ranAbilityType[ranIndex];
                                        inven_equip_datas[(int)User_Equip.abilityValue] = ranValue.ToString();
                                        inven_equip_data[invenIndex] = string.Join("#", inven_equip_datas);

                                        ReturnData += MakePacket(ReturnPacket_.Update_UserInvenEquip, inven_equip_data[invenIndex]);
                                        inven_equip = string.Join("*", inven_equip_data);
                                        ReturnData += await SetUserData(UserId, Characterdata_string.inven_equip_data, inven_equip);

                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Transmute_Tab, equipType);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Transmute_Equip_Detail, index);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | {index} | addMagicType {ranAbilityType[ranIndex]} addMagicValue {ranValue}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다시 시도해 주세요.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Transmute_Merge_Pet:
                        {
                            string petType = value1;

                            string invenIndexGroup = value2;
                            string[] invenIndexGroups = invenIndexGroup.Split('+');

                            if (invenIndexGroups.Length != 5)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "나비가 모두 선택되지 않았습니다.");
                            }
                            else
                            {
                                bool bNotType = false;
                                bool bSetBagSlot = false;
                                Dictionary<string, string[]> petDic = await _redisService.GetTemplateAll_Indexing(TEMPLATE_TYPE.pet);

                                Dictionary<string, int> gradeDic = new Dictionary<string, int>();

                                string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                                string[] bagSlot_ = bagSlot.Split('#');

                                for (int i = 0; i < invenIndexGroups.Length; i++)
                                {
                                    string invenIndex = invenIndexGroups[i];
                                    string invenData = await _redisService.GetPetInven(UserId, invenIndex);

                                    if (invenData == "") continue;
                                    
                                    if(invenIndex == bagSlot_[2] || invenIndex == bagSlot_[3] || invenIndex == bagSlot_[4])
                                    {
                                        bSetBagSlot = true;
                                        break;
                                    }

                                    string[] invenDatas = invenData.Split('#');
                                    string id = invenDatas[(int)User_Pet.id];
                                    string grade = petDic[id][(int)pettemplate_.grade];

                                    if (gradeDic.ContainsKey(grade)) gradeDic[grade]++;
                                    else gradeDic.Add(grade, 1);

                                    if (petType != petDic[id][(int)pettemplate_.type])
                                    {
                                        bNotType = true;
                                        break;
                                    }
                                }

                                if (bNotType == true)
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "같은 그룹의 나비가 아닙니다.");
                                    break;
                                }

                                if (bSetBagSlot == true)
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "착용 중인 나비가 있습니다.");
                                    break;
                                }

                                bool bNotGrade = false;
                                foreach (var gradeData in gradeDic)
                                {
                                    switch (gradeData.Key)
                                    {
                                        case "1": { if (gradeData.Value != 2) bNotGrade = true; } break;
                                        case "2": { if (gradeData.Value != 2) bNotGrade = true; } break;
                                        case "3": { if (gradeData.Value != 1) bNotGrade = true; } break;
                                    }

                                    if (bNotGrade == true) break;
                                }

                                if (bNotGrade == true)
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "등급이 맞지 않습니다.");
                                    break;
                                }

                                if (await CheckGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), invenIndexGroup) == true)
                                {
                                    ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), invenIndexGroup, false);

                                    string randomBox = await GetRandomBox_Pet_Group(petDic, petType);
                                    ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), randomBox, true, true, GachaToastType_.PetMerge);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Transmute_Tab, "2");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | group {petType} | newId {randomBox}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "나비가 부족합니다.");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Transmute_Extraction_Pet:
                        {
                            string invenIndexGroup = value1;
                            string curTransmuteTabType = value2;
                            string[] invenIndexGroups = invenIndexGroup.Split('+');

                            bool BNotGrade = false;

                            List<string> gradeList = new List<string>();

                            if (invenIndexGroup == "")
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "추출할 나비가 없습니다.");
                                break;
                            }

                            for (int i = 0; i < invenIndexGroups.Length; i++)
                            {
                                if (invenIndexGroups[i] == "") continue;

                                string invenIndex = invenIndexGroups[i];
                                string invenData = await _redisService.GetPetInven(UserId, invenIndex);

                                if (invenData == "") continue;

                                string[] invenDatas = invenData.Split('#');
                                string id = invenDatas[(int)User_Pet.id];
                                string grade = invenDatas[(int)User_Pet.grade];

                                if (int.Parse(grade) < 4)
                                {
                                    BNotGrade = true;
                                    break;
                                }

                                gradeList.Add(grade);
                            }

                            if (BNotGrade == true)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "4등급 이하 나비가 있습니다.");
                                break;
                            }

                            if (await CheckGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), invenIndexGroup) == true)
                            {
                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Pet).ToString(), invenIndexGroup, false);

                                string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Transmute_Extraction_Reward).ToString());

                                string grade4RewardPer = systemdic[(int)systemtemplate_.value1];
                                string grade4RewardType = systemdic[(int)systemtemplate_.value2];

                                float[] grade4RewardPer_ = grade4RewardPer.Split('#').Select(s => float.Parse(s)).ToArray();
                                string[] grade4RewardType_ = grade4RewardType.Split('#');

                                string grade5RewardPer = systemdic[(int)systemtemplate_.value3];
                                string grade5RewardType = systemdic[(int)systemtemplate_.value4];

                                float[] grade5RewardPer_ = grade5RewardPer.Split('#').Select(s => float.Parse(s)).ToArray();
                                string[] grade5RewardType_ = grade5RewardType.Split('#');

                                for (int i = 0; i < gradeList.Count; i++)
                                {
                                    string rewardtype = "";

                                    if (gradeList[i] == "4")
                                    {
                                        float rand = GetRandomFloat(0f, 100f);
                                        float cumulative = 0f;

                                        for (int j = 0; j < grade4RewardPer_.Length; j++)
                                        {
                                            cumulative += grade4RewardPer_[j];
                                            if (rand < cumulative)
                                            {
                                                rewardtype = grade4RewardType_[j];
                                                break;
                                            }
                                        }
                                    }
                                    else if (gradeList[i] == "5")
                                    {
                                        float rand = GetRandomFloat(0f, 100f);
                                        float cumulative = 0f;

                                        for (int j = 0; j < grade5RewardPer_.Length; j++)
                                        {
                                            cumulative += grade5RewardPer_[j];
                                            if (rand < cumulative)
                                            {
                                                rewardtype = grade5RewardType_[j];
                                                break;
                                            }
                                        }
                                    }

                                    if (rewardtype != "") ReturnData += await RewardGoods(UserId, rewardtype, "1");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | getRewardType {rewardtype}");
                                }
                                
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Transmute_Tab, curTransmuteTabType);
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "나비가 부족합니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.Hero_Get:
                        {
                            string index = value1;

                            string inven_character = await GetUserData(UserId, Characterdata_string.inven_hero_data);
                            string[] inven_character_data = inven_character.Split('*');

                            bool bHas = false;

                            for (int i = 0; i < inven_character_data.Length; i++)
                            {
                                if (inven_character_data[i] == "") continue;

                                string[] inven_character_datas = inven_character_data[i].Split('#');

                                if (inven_character_datas[(int)User_Hero.index] == index)
                                {
                                    bHas = true;
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "캐릭터가 이미 해금되었습니다.");
                                    break;
                                }
                            }

                            if (bHas == false)
                            {
                                string[] heroDic = await _redisService.GetTemplate(TEMPLATE_TYPE.hero, index);

                                string questIndex = heroDic[(int)herotemplate_.questIndex];
                                string questcount = heroDic[(int)herotemplate_.questcount];
                                string needtype = heroDic[(int)herotemplate_.needtype];
                                string needvalue = heroDic[(int)herotemplate_.needvalue];

                                if (questIndex != "" && questcount != "")
                                {
                                    string curQuestCount = await _redisService.GetQuestCount(UserId, ((int)QuestType_.character).ToString(), questIndex);
                                    if (long.Parse(questcount) <= long.Parse(curQuestCount))
                                    {
                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Hero).ToString(), index);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Character, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | index {index}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "캐릭터 해금이 필요합니다.");
                                    }
                                }
                                else if (needtype != "" && needvalue != "")
                                {
                                    if (await CheckGoods(UserId, needtype, needvalue) == true)
                                    {
                                        ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Reward_Hero).ToString(), index);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Character, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | index {index}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Hero_Change:
                        {
                            string index = value1;

                            string inven_hero = await GetUserData(UserId, Characterdata_string.inven_hero_data);
                            string[] inven_hero_data = inven_hero.Split('*');

                            bool bHas = false;

                            for (int i = 0; i < inven_hero_data.Length; i++)
                            {
                                if (inven_hero_data[i] == "") continue;

                                string[] inven_pet_datas = inven_hero_data[i].Split('#');

                                if (inven_pet_datas[(int)User_Hero.index] == index)
                                {
                                    bHas = true;
                                    break;
                                }
                            }

                            if (bHas == false)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "캐릭터 해금이 필요합니다.");
                            }
                            else
                            {
                                string curHeroSlot = await GetUserData(UserId, Characterdata_int.heroSlot);

                                if (curHeroSlot == index)
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 선택되었습니다.");
                                }
                                else
                                {
                                    ReturnData += await SetUserData(UserId, Characterdata_int.heroSlot, index);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Character, "");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_CharacterIcon, "");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Ranking_Get:
                        {
                            string tabType = value1;
                            string mapindex = value2;
                            string tabIndex = value3;

                            string key = "";
                            if (tabType == "0")
                            {
                                string stageLastIndex = await GetUserData(UserId, Characterdata_int.StageLastIndex);
                                if (int.Parse(mapindex) > int.Parse(stageLastIndex)) 
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "아직 해당 맵이 열리지 않았습니다.");
                                    break;
                                }

                                key = $"{RedisService.RankUnlimitMode}_{mapindex}_{_redisService.GetDateTimeTotalWeek()}";
                            }
                            else if (tabType == "1")
                            {
                                key = $"{RedisService.RankBattleMode}_{_redisService.GetDateTimeTotalWeek()}";
                            }

                            int tabIndex_ = int.Parse(tabIndex);
                            int itemsPerPage = 7;

                            int startIndex = tabIndex_ * itemsPerPage;
                            int endIndex = startIndex + itemsPerPage - 1;

                            var rankingData = await _redisService.GetRankingRange(key, startIndex, endIndex);

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Init, "");

                            int itemCount = 0;
                            foreach (var data in rankingData)
                            {
                                string rankUserId = _redisService.RedisToString(data.Element);
                                double point = data.Score;

                                string nickname = await GetUserData(rankUserId, Characterdata_string.nickname);
                                long rank = await _redisService.GetRanking(key, rankUserId);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Add_Item, nickname, ((long)Math.Floor(point)).ToString(), (rank + 1).ToString());

                                itemCount++;
                            }

                            long myRank = await _redisService.GetRanking(key, UserId);
                            string myNickname = await GetUserData(UserId, Characterdata_string.nickname);

                            if (myRank >= 0)
                            {
                                double myPoint = await _redisService.GetRankingScore(key, UserId);

                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Add_MyItem, myNickname, ((long)Math.Floor(myPoint)).ToString(), (myRank + 1).ToString());
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Add_MyItem, myNickname, "0", "0");
                            }


                            if (itemCount != itemsPerPage) ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Clear, tabType, tabIndex, true.ToString());
                            else ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Clear, tabType, tabIndex, false.ToString());
                        }
                        break;
                    case Totalpacket_type.Ranking_BuyRewardShop:
                        {
                            string templateIndex = value1;

                            string[] shopDic = await _redisService.GetTemplate(TEMPLATE_TYPE.shop, templateIndex);

                            if (shopDic.Length > 0)
                            {
                                string maxcount = shopDic[(int)shoptemplate_.maxcount];
                                string needtype = shopDic[(int)shoptemplate_.needtype];
                                string needvalue = shopDic[(int)shoptemplate_.needvalue];

                                string shopPurchase = await _redisService.GetUserShopPurchase(UserId, templateIndex);
                                if (shopPurchase != "")
                                {
                                    string[] shopPurchase_ = shopPurchase.Split("#");

                                    if (DateTime.Parse(shopPurchase_[(int)User_shopPurchase.nextTimingday]) > DateTime.Now)
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 구매한 상품입니다.");
                                    }
                                    else if (int.Parse(shopPurchase_[(int)User_shopPurchase.buyCount]) >= int.Parse(maxcount))
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최대 구매 횟수입니다.");
                                    }
                                    else
                                    {
                                        if (await CheckGoods(UserId, needtype, needvalue) == true)
                                        {
                                            // 구매 필요 재화 삭감
                                            ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                            // 유저 상점 인벤데이터 초기화
                                            ReturnData += await ProcessUserShopPurchase(UserId, shopDic);

                                            // 보상
                                            string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                            string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                            string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                            string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                            string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                            string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                            string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                            string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                            if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                            if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                            if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                            if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);

                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ranking_Shop, "");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                            WriteLog(UserId, $"{(Totalpacket_type)type_} | index {templateIndex}");
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Attendance_Reward:
                        {
                            string index = value1;

                            string attendanceData = await GetUserData(UserId, Characterdata_string.attendance7Days);
                            string[] attendanceDatas = attendanceData.Split('*');

                            string lastAttendanceDate = attendanceDatas[(int)User_Attendance7days.lastAttendanceDate];
                            string currentDayIndex = attendanceDatas[(int)User_Attendance7days.currentDayIndex];
                            string[] rewardDayIndex = attendanceDatas[(int)User_Attendance7days.rewardDayIndex].Split('#');

                            int Index_ = int.Parse(index);
                            int nextIndex = 0;

                            if (lastAttendanceDate == DateTime.Now.ToString("yyyy-MM-dd"))
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"이미 오늘 보상을 수령했습니다.");
                                break;
                            }

                            if (rewardDayIndex[Index_] == "1")
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"이미 {Index_ + 1}일차 보상을 수령했습니다.");
                                break;
                            }

                            for (int i = 0; i < 7; i++)
                            {
                                if (rewardDayIndex[i] == "0")
                                {
                                    nextIndex = i;
                                    break;
                                }
                            }

                            if (nextIndex == Index_)
                            {
                                string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Attendance7daysEventReward).ToString());

                                string reward = systemdic[(int)systemtemplate_.value1 + nextIndex];
                                string[] rewards = reward.Split('#');
                                string rewardtype = rewards[0];
                                string rewardvalue = rewards[1];

                                ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);

                                rewardDayIndex[nextIndex] = "1";
                                attendanceDatas[(int)User_Attendance7days.lastAttendanceDate] = DateTime.Now.ToString("yyyy-MM-dd");
                                attendanceDatas[(int)User_Attendance7days.currentDayIndex] = (nextIndex + 1).ToString();
                                attendanceDatas[(int)User_Attendance7days.rewardDayIndex] = string.Join("#", rewardDayIndex);
                                ReturnData += await SetUserData(UserId, Characterdata_string.attendance7Days, string.Join("*", attendanceDatas));
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Attendance7Days, "");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                WriteLog(UserId, $"{(Totalpacket_type)type_} | index {index}");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"{nextIndex + 1}일차 보상 부터 수령할 수 있습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.MainQuestState_Set:
                        {
                            string mainQuestState = await GetUserData(UserId, Characterdata_int.mainQuestState);
                            string mainQuestIndex = await GetUserData(UserId, Characterdata_int.mainQuestIndex);
                            string mainQuestCount = await GetUserData(UserId, Characterdata_int.mainQuestCount);

                            //start
                            string[] mainquestDic = await _redisService.GetTemplate(TEMPLATE_TYPE.mainquest, mainQuestIndex);
                            string dialogIndexs = mainquestDic[(int)mainquesttemplate_.dialogindex];
                            string checkQuestRewardType = mainquestDic[(int)mainquesttemplate_.checkquestrewardtype];

                            if (mainQuestState == "0")
                            {
                                string questindex = mainquestDic[(int)mainquesttemplate_.questindex];

                                string curMainQuestCount = "0";
                                if (checkQuestRewardType != "")
                                {
                                    string[] goodsDic = await _redisService.GetTemplate(TEMPLATE_TYPE.goods, checkQuestRewardType);

                                    if (goodsDic != null)
                                    {
                                        string linkInt = goodsDic[(int)goodstemplate_.link_int];
                                        curMainQuestCount = await GetUserData(UserId, (Characterdata_int)int.Parse(linkInt));
                                    }
                                }

                                ReturnData += await SetUserData(UserId, Characterdata_int.mainQuestState, "1");
                                ReturnData += await SetUserData(UserId, Characterdata_int.mainQuestCount, curMainQuestCount);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Quest, "0");

                                WriteLog(UserId, $"{(Totalpacket_type)type_} | ingIndex {mainQuestIndex}");

                                if (dialogIndexs != "")
                                {
                                    string startDialogIndexs = dialogIndexs.Split('*')[0];
                                    if (startDialogIndexs != "") ReturnData += MakePacket(ReturnPacket_.UpdateUI_Dialog, startDialogIndexs);
                                }
                            }
                            //reward
                            else if (mainQuestState == "1")
                            {
                                string nextlink = mainquestDic[(int)mainquesttemplate_.nextlink];
                                string rewardtype = mainquestDic[(int)mainquesttemplate_.rewardtype];
                                string rewardvalue = mainquestDic[(int)mainquesttemplate_.rewardvalue];
                                string rewardtype2 = mainquestDic[(int)mainquesttemplate_.rewardtype2];
                                string rewardvalue2 = mainquestDic[(int)mainquesttemplate_.rewardvalue2];
                                string rewardtype3 = mainquestDic[(int)mainquesttemplate_.rewardtype3];
                                string rewardvalue3 = mainquestDic[(int)mainquesttemplate_.rewardvalue3];
                                string needCount = mainquestDic[(int)mainquesttemplate_.needcount];
                                string questindex = mainquestDic[(int)mainquesttemplate_.questindex];

                                if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord2) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore2);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord3) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore3);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord4) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore4);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord5) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore5);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord6) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore6);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord7) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore7);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord8) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore8);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord9) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore9);
                                else if (int.Parse(questindex) == (int)QuestIndex_.UnlimitFlyingHighRecord10) mainQuestCount = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore10);
                                else if (int.Parse(questindex) == (int)QuestIndex_.GetAether)
                                {
                                    string aetherInven = await GetUserData(UserId, Characterdata_string.inven_aether_data);
                                    if (aetherInven != "") mainQuestCount = (aetherInven.Split('*').Length).ToString();
                                }
                                else
                                {
                                    if (checkQuestRewardType != "") 
                                    {
                                        string[] goodsDic = await _redisService.GetTemplate(TEMPLATE_TYPE.goods, checkQuestRewardType);
                                        
                                        if (goodsDic != null)
                                        {
                                            string linkInt = goodsDic[(int)goodstemplate_.link_int];
                                            mainQuestCount = await GetUserData(UserId, (Characterdata_int)int.Parse(linkInt));
                                        }
                                    }
                                }

                                if (long.Parse(mainQuestCount) >= long.Parse(needCount))
                                {
                                    if (dialogIndexs != "" && dialogIndexs.Contains('*'))
                                    {
                                        string endDialogIndexs = dialogIndexs.Split('*')[1];
                                        if (endDialogIndexs != "") ReturnData += MakePacket(ReturnPacket_.UpdateUI_Dialog, endDialogIndexs);
                                    }

                                    if (checkQuestRewardType != "")
                                    {
                                        ReturnData += await RewardGoods(UserId, checkQuestRewardType, needCount, false);

                                        if (checkQuestRewardType == ((int)GoodsType_.Material_MemoryPiece).ToString()) 
                                        {
                                            string stageLastIndex = await GetUserData(UserId, Characterdata_int.StageLastIndex);
                                            if (int.Parse(stageLastIndex) < 2) ReturnData += await SetUserData(UserId, Characterdata_int.StageLastIndex, "2");
                                        }
                                    }

                                    ReturnData += await SetUserData(UserId, Characterdata_int.mainQuestState, "0");
                                    ReturnData += await SetUserData(UserId, Characterdata_int.mainQuestIndex, nextlink);
                                    ReturnData += await SetUserData(UserId, Characterdata_int.mainQuestCount, "0");

                                    bool bPopupReward1 = true;
                                    bool bPopupReward2 = true;
                                    bool bPopupReward3 = true;

                                    if (rewardtype == ((int)GoodsType_.BeginnerBuff).ToString()) bPopupReward1 = false;
                                    if (rewardtype2 == ((int)GoodsType_.BeginnerBuff).ToString()) bPopupReward2 = false;
                                    if (rewardtype3 == ((int)GoodsType_.BeginnerBuff).ToString()) bPopupReward3 = false;

                                    if (rewardtype != "" && rewardvalue != "") ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue, true, bPopupReward1);
                                    if (rewardtype2 != "" && rewardvalue2 != "") ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2, true, bPopupReward2);
                                    if (rewardtype3 != "" && rewardvalue3 != "") ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3, true, bPopupReward3);

                                    if (rewardtype == ((int)GoodsType_.BeginnerBuff).ToString() || rewardtype2 == ((int)GoodsType_.BeginnerBuff).ToString() || rewardtype3 == ((int)GoodsType_.BeginnerBuff).ToString())
                                    {
                                        string[] goodsDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.goods, ((int)GoodsType_.BeginnerBuff).ToString());
                                        string icon = goodsDic[(int)goodstemplate_.icon];
                                        string name = goodsDic[(int)goodstemplate_.name];
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Skill_Get, icon, $"{name}를 획득했습니다.");
                                    }

                                    if (rewardtype == ((int)GoodsType_.Reward_Hero).ToString() ||
                                        rewardtype2 == ((int)GoodsType_.Reward_Hero).ToString() ||
                                        rewardtype3 == ((int)GoodsType_.Reward_Hero).ToString()) 
                                    {
                                        // 까로 획득시
                                        ReturnData += await SetUserData(UserId, Characterdata_string.reddot_Hero, "2");
                                    }

                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Quest, "0");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                    
                                    ReturnData += await CheckTutorial(UserId, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | clearIndex {mainQuestIndex}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"스토리 조건이 충족되지 않았습니다.\n[{mainQuestCount}/{needCount}]");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.DailyQuestPointReward:
                        {
                            string index = value1;

                            string dailyQuestPoint = await GetUserData(UserId, Characterdata_int.dailyQuestPoint);
                            string dailyQuestReward = await GetUserData(UserId, Characterdata_string.dailyQuestReward);
                            string[] dailyQuestRewards = dailyQuestReward.Split('#');

                            string[] questDic = await _redisService.GetTemplate(TEMPLATE_TYPE.quest, index);
                            string questid = questDic[(int)questtemplate_.questid];
                            string needCount = questDic[(int)questtemplate_.needcount];

                            if (long.Parse(dailyQuestPoint) >= long.Parse(needCount))
                            {
                                if (dailyQuestRewards[int.Parse(questid)] != "1")
                                {
                                    dailyQuestRewards[int.Parse(questid)] = "1";

                                    string rewardtype = questDic[(int)questtemplate_.rewardtype];
                                    string rewardvalue = questDic[(int)questtemplate_.rewardvalue];

                                    ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                    ReturnData += await SetUserData(UserId, Characterdata_string.dailyQuestReward, string.Join("#", dailyQuestRewards));
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Quest, "1");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_QuestPointRewardBoxAni, "");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | Index {index}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"이미 보상을 수령하였습니다.");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"포인트가 부족합니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.DailyQuestReward:
                        {
                            string index = value1;

                            string[] questDic = await _redisService.GetTemplate(TEMPLATE_TYPE.quest, index);
                            string questIndex = questDic[(int)questtemplate_.questindex];
                            string questid = questDic[(int)questtemplate_.questid];
                            string needCount = questDic[(int)questtemplate_.needcount];
                            string rewardpoint = questDic[(int)questtemplate_.rewardpoint];

                            string dailyData = await _redisService.GetQuestData(UserId, ((int)QuestType_.daily).ToString(), questIndex);

                            if (dailyData == questid)
                            {
                                string dailyCount = await _redisService.GetQuestCount(UserId, ((int)QuestType_.daily).ToString(), questIndex);

                                if (long.Parse(dailyCount) >= long.Parse(needCount))
                                {
                                    long curPointValue = long.Parse(await GetUserData(UserId, Characterdata_int.dailyQuestPoint));
                                    long setPointValue = curPointValue + long.Parse(rewardpoint);

                                    ReturnData += await RewardGoods(UserId, ((int)GoodsType_.dailyQuestPoint).ToString(), rewardpoint, true, false);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_QuestPointRewardSliderAni, curPointValue.ToString(), setPointValue.ToString(), index);

                                    dailyData = (long.Parse(dailyData) + 1).ToString();
                                    await _redisService.SetQuestData(UserId, ((int)QuestType_.daily).ToString(), questIndex, dailyData);
                                    ReturnData += MakePacket(ReturnPacket_.Update_QuestData, ((int)QuestType_.daily).ToString(), questIndex, dailyData);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Quest, "1");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | Index {index}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"조건이 충족되지 않았습니다.\n[{dailyCount}/{needCount}]");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"다시 시도해 주세요.");
                            }
                        }
                        break;
                    case Totalpacket_type.TutorialIndex:
                        {
                            string index = value1;

                            ReturnData += await CheckTutorial(UserId, index);
                        }
                        break;
                    case Totalpacket_type.FlyingTicket_Check:
                        {
                            ReturnData += await CheckFlyingTicket(UserId);
                        }
                        break;
                    case Totalpacket_type.FlyingTicket_Buy:
                        {
                            string buytype = value1;

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.flyingTicket).ToString());

                            string ticketMaxCount = systemdic[(int)systemtemplate_.value1];
                            string GivenTime = systemdic[(int)systemtemplate_.value2];
                            string AdTryMaxCount = systemdic[(int)systemtemplate_.value3];

                            string curTicketCount = await GetUserData(UserId, Characterdata_int.FlyingTicket);
                            string curFlyingTicket_AdTry = await GetUserData(UserId, Characterdata_int.FlyingTicket_AdTry);

                            int ticketMaxCount_ = int.Parse(ticketMaxCount);
                            int GivenTime_ = int.Parse(GivenTime);
                            int AdTryMaxCount_ = int.Parse(AdTryMaxCount);
                            int curTicketCount_ = int.Parse(curTicketCount);
                            int curFlyingTicket_AdTry_ = int.Parse(curFlyingTicket_AdTry);

                            if (curTicketCount_ <= 0) 
                            {
                                // 광고
                                if (buytype == "1")
                                {
                                    if (curFlyingTicket_AdTry_ < AdTryMaxCount_)
                                    {
                                        ReturnData += await SetUserData(UserId, Characterdata_int.FlyingTicket_AdTry, (curFlyingTicket_AdTry_ + 1).ToString());
                                     
                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.FlyingTicket).ToString(), "1");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | buytype  {buytype}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"광고 보기 남은 횟수를 초과하였습니다.");
                                    }
                                }
                                // 멜라 구매
                                else if (buytype == "2")
                                {
                                    string needType = ((int)GoodsType_.Mela).ToString();
                                    string needValue = systemdic[(int)systemtemplate_.value4];
                                    string flyingTicket_BuyCount = await GetUserData(UserId, Characterdata_int.FlyingTicket_BuyCount);
                                    long flyingTicket_BuyCount_ = long.Parse(flyingTicket_BuyCount);

                                    if (flyingTicket_BuyCount_ < 3) needValue = systemdic[(int)systemtemplate_.value4];
                                    else if (flyingTicket_BuyCount_ < 5) needValue = systemdic[(int)systemtemplate_.value5];
                                    else needValue = systemdic[(int)systemtemplate_.value6];

                                    if (await CheckGoods(UserId, needType, needValue) == true)
                                    {
                                        ReturnData += await RewardGoods(UserId, needType, needValue, false);

                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.FlyingTicket).ToString(), "2");
                                        ReturnData += await SetUserData(UserId, Characterdata_int.FlyingTicket_BuyCount, (flyingTicket_BuyCount_ + 1).ToString());
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | buytype  {buytype}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"이미 비행권을 구매하였습니다.");
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_TicketPoup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Ticket, "");
                        }
                        break;
                    case Totalpacket_type.UnlimitFlyingStart:
                        {
                            string stageIndex = value1;

                            string curTicketCount = await GetUserData(UserId, Characterdata_int.FlyingTicket);
                            int curTicketCount_ = int.Parse(curTicketCount);

                            if (curTicketCount_ > 0)
                            { 
                                ReturnData += await SetUserData(UserId, Characterdata_int.FlyingTicket, (curTicketCount_-1).ToString());

                                int stageIndex_ = int.Parse(stageIndex);
                                if (stageIndex_ > 1)
                                {
                                    QuestIndex_ questindex = QuestIndex_.none;
                                    
                                    if (stageIndex_ == 2) questindex = QuestIndex_.FlyingMap2;
                                    else if (stageIndex_ == 3) questindex = QuestIndex_.FlyingMap3;
                                    else if (stageIndex_ == 4) questindex = QuestIndex_.FlyingMap4;
                                    else if (stageIndex_ == 5) questindex = QuestIndex_.FlyingMap5;
                                    else if (stageIndex_ == 6) questindex = QuestIndex_.FlyingMap6;
                                    else if (stageIndex_ == 7) questindex = QuestIndex_.FlyingMap7;
                                    else if (stageIndex_ == 8) questindex = QuestIndex_.FlyingMap8;
                                    else if (stageIndex_ == 9) questindex = QuestIndex_.FlyingMap9;
                                    else if (stageIndex_ == 10) questindex = QuestIndex_.FlyingMap10;

                                    ReturnData += await OnQuest(UserId, questindex, 1);
                                    ReturnData += await OnQuest(UserId, QuestIndex_.PlayGame, 1);
                                }
                                else
                                {
                                    ReturnData += await OnQuest(UserId, QuestIndex_.PlayGame, 1);
                                }

                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_MoveBattle, "");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"비행권이 부족합니다.");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_DestoryWaitingGame, "");
                            }
                        }
                        break;
                    case Totalpacket_type.UnlimitFlyingEnd:
                        {
                            string posX = value1;
                            string hp = value2;
                            string stageIndex = value3;
                            float posX_ = float.Parse(posX);

                            if (hp == "0") 
                            {
                                bool bHighScore = false;
                                int addRankingPoint = 0;
                                int rank_ = 0;

                                await _redisService.AddRanking_UnlimitHighScore(UserId, (long)posX_, stageIndex);

                                int stageIndex_ = int.Parse(stageIndex);
                                if (stageIndex_ > 1)
                                {
                                    QuestIndex_ questindex = QuestIndex_.none;
                                    Characterdata_int characterInt = Characterdata_int.none;

                                    if (stageIndex_ == 2)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord2;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore2;
                                    }
                                    else if (stageIndex_ == 3)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord3;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore3;
                                    }
                                    else if (stageIndex_ == 4)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord4;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore4;
                                    }
                                    else if (stageIndex_ == 5)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord5;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore5;
                                    }
                                    else if (stageIndex_ == 6)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord6;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore6;
                                    }
                                    else if (stageIndex_ == 7)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord7;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore7;
                                    }
                                    else if (stageIndex_ == 8)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord8;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore8;
                                    }
                                    else if (stageIndex_ == 9)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord9;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore9;
                                    }
                                    else if (stageIndex_ == 10)
                                    {
                                        questindex = QuestIndex_.UnlimitFlyingHighRecord10;
                                        characterInt = Characterdata_int.UnlimitFlyingHighScore10;
                                    }

                                    ReturnData += await OnQuest(UserId, questindex, 1);
                                    ReturnData += await OnQuest(UserId, QuestIndex_.UnlimitFlyingAddRecord, (long)posX_);

                                    string curScore = await GetUserData(UserId, characterInt);
                                    if (long.Parse(curScore) < (long)posX_)
                                    {
                                        bHighScore = true;
                                        ReturnData += await SetUserData(UserId, characterInt, ((long)posX_).ToString());
                                    }
                                }
                                else
                                {
                                    ReturnData += await OnQuest(UserId, QuestIndex_.UnlimitFlyingHighRecord, (long)posX_);
                                    ReturnData += await OnQuest(UserId, QuestIndex_.UnlimitFlyingAddRecord, (long)posX_);

                                    string curScore = await GetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore);
                                    if (long.Parse(curScore) < (long)posX_)
                                    {
                                        bHighScore = true;
                                        ReturnData += await SetUserData(UserId, Characterdata_int.UnlimitFlyingHighScore, ((long)posX_).ToString());
                                    }
                                }

                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_BattleClearPopup, bHighScore ? "1" : "0");
                            }
                        }
                        break;
                    case Totalpacket_type.Reddot_Del:
                        {
                            string stringLink = value1;
                            int stringLink_ = int.Parse(stringLink);

                            ReturnData += await SetUserData(UserId, (Characterdata_string)stringLink_, "");
                        }
                        break;
                    case Totalpacket_type.Altar_Set:
                        {
                            string buytype = value1;

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Altar_RandomAblility).ToString());

                            string randomPer = systemdic[(int)systemtemplate_.value1];
                            string abilityType = systemdic[(int)systemtemplate_.value2];
                            string abilityValue = systemdic[(int)systemtemplate_.value3];
                            string roomBuyValue = systemdic[(int)systemtemplate_.value4];
                            string AdTryMaxCount = systemdic[(int)systemtemplate_.value5];

                            string curAltar_AdTry = await GetUserData(UserId, Characterdata_int.Altar_AdTry);

                            int AdTryMaxCount_ = int.Parse(AdTryMaxCount);
                            int curAltar_AdTry_ = int.Parse(curAltar_AdTry);

                            bool bAble = false;

                            // 광고
                            if (buytype == "1")
                            {
                                if (curAltar_AdTry_ < AdTryMaxCount_)
                                {
                                    ReturnData += await SetUserData(UserId, Characterdata_int.Altar_AdTry, (curAltar_AdTry_ + 1).ToString());

                                    bAble = true;
                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | buytype  {buytype}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"광고 보기 남은 횟수를 초과하였습니다.");
                                }
                            }
                            // 멜라 구매
                            else if (buytype == "2")
                            {
                                string needType = ((int)GoodsType_.Room).ToString();

                                if (await CheckGoods(UserId, needType, roomBuyValue) == true)
                                {
                                    ReturnData += await RewardGoods(UserId, needType, roomBuyValue, false);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                    bAble = true;

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | buytype  {buytype}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                }
                            }

                            if (bAble) 
                            {
                                string curAltar = await GetUserData(UserId, Characterdata_string.altarSet);

                                List<float> randomPerFloat = randomPer.Split('#').Select(s => float.Parse(s)).ToList();
                                List<string> abilityTypes = abilityType.Split('#').ToList();
                                List<string> abilityValues = abilityValue.Split('#').ToList();

                                if (curAltar != "")
                                {
                                    string curAltarType = curAltar.Split('#')[0];
                                    int curAltarIndex = -1;
                                    for (int i = 0; i < abilityTypes.Count; i++)
                                    {
                                        if (abilityTypes[i] == curAltarType)
                                        {
                                            curAltarIndex = i;
                                            break;
                                        }
                                    }

                                    if (curAltarIndex >= 0)
                                    {
                                        randomPerFloat.RemoveAt(curAltarIndex);
                                        abilityTypes.RemoveAt(curAltarIndex);
                                        abilityValues.RemoveAt(curAltarIndex);
                                    }
                                }

                                float totalPer = 0;
                                for (int i = 0; i < randomPerFloat.Count; i++) totalPer += randomPerFloat[i];

                                float rand = GetRandomFloat(0, totalPer);
                                float cumulative = 0f;

                                for (int i = 0; i < randomPerFloat.Count; i++)
                                {
                                    cumulative += randomPerFloat[i];
                                    if (rand < cumulative)
                                    {
                                        string altarSet = $"{abilityTypes[i]}#{abilityValues[i]}";
                                        ReturnData += await SetUserData(UserId, Characterdata_string.altarSet, altarSet);
                                        ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.ui_reward).ToString());
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Altar, "");
                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Cave_ResetSkill:
                        {
                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.CaveReset).ToString());

                            string resetNeedType = systemdic[(int)systemtemplate_.value1];
                            string resetNeedValue = systemdic[(int)systemtemplate_.value2];

                            if (await CheckGoods(UserId, resetNeedType, resetNeedValue)) 
                            {
                                Dictionary<string, string[]> skillDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.skill);

                                ReturnData += await RewardGoods(UserId, resetNeedType, resetNeedValue, false);

                                string shockGroup = await GetUserData(UserId, Characterdata_int.ShockGroup);
                                string shockType1 = await GetUserData(UserId, Characterdata_int.ShockType1);
                                string shockType2 = await GetUserData(UserId, Characterdata_int.ShockType2);

                                int shockType1_ = int.Parse(shockType1);
                                int shockType2_ = int.Parse(shockType2);

                                if (shockGroup != "0") 
                                {
                                    Dictionary<int, long> returnRewardDic = new Dictionary<int, long>();
                                    foreach (var data in skillDic)
                                    {
                                        string index = data.Value[(int)skilltemplate_.index];
                                        string group = data.Value[(int)skilltemplate_.group];
                                        string skillType = data.Value[(int)skilltemplate_.type];
                                        string skillLevel = data.Value[(int)skilltemplate_.level];
                                        int skillLevel_ = int.Parse(skillLevel);

                                        if (group == "0") continue;
                                        if (group != shockGroup) continue;

                                        if (skillType == "1" && skillLevel_ <= shockType1_)
                                        {
                                            for (int i = (int)skilltemplate_.needtype; i <= (int)skilltemplate_.needvalue3; i = i + 2)
                                            {
                                                string needType = skillDic[index][i];
                                                string needValue = skillDic[index][i + 1];

                                                if (needType != "" && needValue != "")
                                                {
                                                    if (returnRewardDic.ContainsKey(int.Parse(needType)) == false) returnRewardDic.Add(int.Parse(needType), long.Parse(needValue));
                                                    else returnRewardDic[int.Parse(needType)] += long.Parse(needValue);
                                                }
                                            }
                                        }
                                        else if (skillType == "2" && skillLevel_ <= shockType2_)
                                        {
                                            for (int i = (int)skilltemplate_.needtype; i <= (int)skilltemplate_.needvalue3; i = i + 2)
                                            {
                                                string needType = skillDic[index][i];
                                                string needValue = skillDic[index][i + 1];

                                                if (needType != "" && needValue != "")
                                                {
                                                    if (returnRewardDic.ContainsKey(int.Parse(needType)) == false) returnRewardDic.Add(int.Parse(needType), long.Parse(needValue));
                                                    else returnRewardDic[int.Parse(needType)] += long.Parse(needValue);
                                                }
                                            }
                                        }
                                    }

                                    foreach (var returnReward in returnRewardDic)
                                    {
                                        ReturnData += await RewardGoods(UserId, returnReward.Key.ToString(), returnReward.Value.ToString());
                                    }

                                    ReturnData += await SetUserData(UserId, Characterdata_int.ShockGroup, "0");
                                    ReturnData += await SetUserData(UserId, Characterdata_int.ShockType1, "0");
                                    ReturnData += await SetUserData(UserId, Characterdata_int.ShockType2, "0");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Cave, "");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "초기화 완료 되었습니다.");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "현재 초기화 상태입니다.");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.Cave_SetSkill:
                        {
                            string index = value1;

                            string[] skillDic = await _redisService.GetTemplate(TEMPLATE_TYPE.skill, index);
                            string group = skillDic[(int)skilltemplate_.group];
                            string skillType = skillDic[(int)skilltemplate_.type];
                            string skillLevel = skillDic[(int)skilltemplate_.level];
                            int skillLevel_ = int.Parse(skillLevel);

                            string shockAuto = await GetUserData(UserId, Characterdata_int.ShockAuto);
                            string shockGroup = await GetUserData(UserId, Characterdata_int.ShockGroup);
                            string shockType1 = await GetUserData(UserId, Characterdata_int.ShockType1);
                            string shockType2 = await GetUserData(UserId, Characterdata_int.ShockType2);

                            int shockType1_ = int.Parse(shockType1);
                            int shockType2_ = int.Parse(shockType2);

                            bool bAble = true;
                            Dictionary<int, long> returnRewardDic = new Dictionary<int, long>();

                            // 오토스킬
                            if (group == "0" ) 
                            {
                                if (skillType == "1" && shockAuto != "1") 
                                {
                                    for (int i = (int)skilltemplate_.needtype; i <= (int)skilltemplate_.needvalue3; i = i + 2)
                                    {
                                        string needType = skillDic[i];
                                        string needValue = skillDic[i + 1];

                                        if (needType != "" && needValue != "")
                                        {
                                            if (await CheckGoods(UserId, needType, needValue))
                                            {
                                                if (returnRewardDic.ContainsKey(int.Parse(needType)) == false) returnRewardDic.Add(int.Parse(needType), long.Parse(needValue));
                                                else returnRewardDic[int.Parse(needType)] += long.Parse(needValue);
                                            }
                                            else
                                            {
                                                bAble = false;
                                                break;
                                            }
                                        }
                                    }

                                    if (bAble)
                                    {
                                        ReturnData += await SetUserData(UserId, Characterdata_int.ShockAuto, "1");

                                        foreach (var returnReward in returnRewardDic)
                                        {
                                            ReturnData += await RewardGoods(UserId, returnReward.Key.ToString(), returnReward.Value.ToString(), false);
                                        }
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Cave, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_CaveBtn, group, skillType, skillLevel);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                        ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.effect_getskill).ToString());

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | index : {index}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                                else if (skillType == "2" )
                                {
                                    if (shockAuto == "1") 
                                    {
                                        for (int i = (int)skilltemplate_.needtype; i <= (int)skilltemplate_.needvalue3; i = i + 2)
                                        {
                                            string needType = skillDic[i];
                                            string needValue = skillDic[i + 1];

                                            if (needType != "" && needValue != "")
                                            {
                                                if (await CheckGoods(UserId, needType, needValue))
                                                {
                                                    if (returnRewardDic.ContainsKey(int.Parse(needType)) == false) returnRewardDic.Add(int.Parse(needType), long.Parse(needValue));
                                                    else returnRewardDic[int.Parse(needType)] += long.Parse(needValue);
                                                }
                                                else
                                                {
                                                    bAble = false;
                                                    break;
                                                }
                                            }
                                        }

                                        if (bAble)
                                        {
                                            ReturnData += await SetUserData(UserId, Characterdata_int.ShockBooster, skillLevel);

                                            foreach (var returnReward in returnRewardDic)
                                            {
                                                ReturnData += await RewardGoods(UserId, returnReward.Key.ToString(), returnReward.Value.ToString(), false);
                                            }
                                            long nextLevel = skillLevel_ + 1;

                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Cave, "");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_CaveBtn, group, skillType, nextLevel.ToString());
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                            ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.effect_getskill).ToString());

                                            WriteLog(UserId, $"{(Totalpacket_type)type_} | index : {index}");
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                        }
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이전 스킬 먼저 업그레이드 가능합니다.");
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 배운 스킬입니다.");
                                }
                            }
                            else
                            {
                                if (shockGroup == "0" || shockGroup == group)
                                {
                                    if (skillType == "1" && shockType1_ + 1 == skillLevel_ ||
                                        skillType == "2" && shockType2_ + 1 == skillLevel_)
                                    {
                                        for (int i = (int)skilltemplate_.needtype; i <= (int)skilltemplate_.needvalue3; i = i + 2)
                                        {
                                            string needType = skillDic[i];
                                            string needValue = skillDic[i + 1];

                                            if (needType != "" && needValue != "")
                                            {
                                                if (await CheckGoods(UserId, needType, needValue))
                                                {
                                                    if (returnRewardDic.ContainsKey(int.Parse(needType)) == false) returnRewardDic.Add(int.Parse(needType), long.Parse(needValue));
                                                    else returnRewardDic[int.Parse(needType)] += long.Parse(needValue);
                                                }
                                                else
                                                {
                                                    bAble = false;
                                                    break;
                                                }
                                            }
                                        }

                                        if (bAble)
                                        {
                                            if (skillType == "2" && shockType1_ <= 0)
                                            {
                                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이전 스킬 먼저 업그레이드 가능합니다.");
                                            }
                                            else
                                            {
                                                if (shockGroup == "0") ReturnData += await SetUserData(UserId, Characterdata_int.ShockGroup, group);

                                                if (skillType == "1") ReturnData += await SetUserData(UserId, Characterdata_int.ShockType1, skillLevel);
                                                else if (skillType == "2") ReturnData += await SetUserData(UserId, Characterdata_int.ShockType2, skillLevel);

                                                foreach (var returnReward in returnRewardDic)
                                                {
                                                    ReturnData += await RewardGoods(UserId, returnReward.Key.ToString(), returnReward.Value.ToString(), false);
                                                }

                                                long nextLevel = skillLevel_ + 1;

                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Cave, "");
                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_CaveBtn, group, skillType, nextLevel.ToString());
                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                                ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.effect_getskill).ToString());

                                                WriteLog(UserId, $"{(Totalpacket_type)type_} | index : {index}");
                                            }
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                        }
                                    }
                                    else
                                    {
                                        if (skillType == "1" && shockType1_ + 1 > 5 || skillType == "2" && shockType2_ + 1 > 5)
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최고레벨 입니다.");
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이전 레벨 먼저 업그레이드 가능합니다.");
                                        }
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "다른 라인의 계열은 배울 수 없습니다.");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.BattleWait_Get:
                        {
                            string battleInspection = await _redisService.GetBattleInspection();
                            if (battleInspection == "1")
                            {
                                string battleInspectionNoti = await _redisService.GetBattleInspectionNoti();

                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, battleInspectionNoti);
                                break;
                            }

                            HashEntry[] WaitRoomEndTime = await _redisService.GetWaitRoomEndTimeAll();
                            
                            for (int i = 0; i < WaitRoomEndTime.Length; i++)
                            {
                                string roomId = _redisService.RedisToString(WaitRoomEndTime[i].Name);
                                string endTime = _redisService.RedisToString(WaitRoomEndTime[i].Value);

                                // 1초 여유 두고 넘었으면
                                if (DateTime.Parse(endTime).AddSeconds(1) < DateTime.Now) 
                                {
                                    await _redisService.DelWaitRoomClients(roomId);
                                    await _redisService.DelWaitRoomTitle(roomId);
                                    await _redisService.DelWaitRoomEndTime(roomId);
                                }
                            }

                            Dictionary<string, List<string>> waitRoomDic = new Dictionary<string, List<string>>();

                            WaitRoomEndTime = await _redisService.GetWaitRoomEndTimeAll();
                            HashEntry[] WaitRoomTitle = await _redisService.GetWaitRoomTitleAll();
                            HashEntry[] WaitRoomClients = await _redisService.GetWaitRoomClientsAll();

                            for (int i = 0; i < WaitRoomEndTime.Length; i++)
                            {
                                string roomId = _redisService.RedisToString(WaitRoomEndTime[i].Name);
                                string endTime = _redisService.RedisToString(WaitRoomEndTime[i].Value);

                                // 처음에는 없는거만 체크
                                if (endTime != "" && waitRoomDic.ContainsKey(roomId) == false)
                                {
                                    waitRoomDic[roomId] = new List<string>();
                                    waitRoomDic[roomId].Add(endTime);
                                }
                            }

                            for (int i = 0; i < WaitRoomTitle.Length; i++)
                            {
                                string roomId = _redisService.RedisToString(WaitRoomTitle[i].Name);
                                string title = _redisService.RedisToString(WaitRoomTitle[i].Value);

                                if (title != "" && waitRoomDic.ContainsKey(roomId) == true) waitRoomDic[roomId].Add(title);
                            }

                            for (int i = 0; i < WaitRoomClients.Length; i++)
                            {
                                string roomId = _redisService.RedisToString(WaitRoomClients[i].Name);
                                string client = _redisService.RedisToString(WaitRoomClients[i].Value);

                                if (client != "" && waitRoomDic.ContainsKey(roomId) == true)
                                {
                                    string[] clients = client.Split('#');
                                    waitRoomDic[roomId].Add(clients.Length.ToString());
                                } 
                            }

                            foreach (var waitRoomData in waitRoomDic)
                            {
                                string roomId = waitRoomData.Key;
                                List<string> dataList = waitRoomData.Value;

                                if (dataList.Count < 3) continue;

                                string endTime = dataList[0];
                                string roomTitle = dataList[1];
                                string playerCount = dataList[2];

                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_BattleRoom_AddPartyItem, roomId, roomTitle, playerCount, endTime);
                            }
                        }
                        break;
                    case Totalpacket_type.Check_AdsSkipTicket:
                        {
                            string adsType = value1;

                            string curAdsTicket = await GetUserData(UserId, Characterdata_int.AdSkipTicket);
                            long curadsTicket_ = long.Parse(curAdsTicket);

                            if (curadsTicket_ > 0)
                            {
                                ReturnData += await SetUserData(UserId, Characterdata_int.AdSkipTicket, (curadsTicket_ - 1).ToString());
                                ReturnData += MakePacket(ReturnPacket_.Update_AdsSkipCheck, adsType);
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.DailyShop_Buy:
                        {
                            string index = value1;

                            string dailyShop = await GetUserData(UserId, Characterdata_string.dailyShop);
                            string[] dailyShopData = dailyShop.Split('*');
                            string[] dailyShopDatas = dailyShopData[int.Parse(index)].Split('#');

                            string templateIndex = dailyShopDatas[0];
                            string discountPer = dailyShopDatas[1];

                            string[] shopDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.shop, templateIndex);

                            if (shopDic.Length > 0)
                            {
                                string maxcount = shopDic[(int)shoptemplate_.maxcount];
                                string needtype = shopDic[(int)shoptemplate_.needtype];
                                string needvalue = shopDic[(int)shoptemplate_.needvalue];
                                
                                if (needvalue != "")
                                {
                                    long needvalue_ = long.Parse(needvalue);
                                    long discountPer_ = long.Parse(discountPer);
                                    needvalue = (needvalue_ - (needvalue_ * discountPer_ * 0.01f)).ToString();
                                }

                                if (int.Parse(dailyShopDatas[2]) >= int.Parse(maxcount))
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최대 구매 횟수입니다.");
                                }
                                else
                                {
                                    // 무료, 광고
                                    if (needtype == "0")
                                    {
                                        // 보상
                                        string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                        string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                        string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                        string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                        string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                        string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                        string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                        string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                        if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                        if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                        if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                        if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);

                                        dailyShopDatas[2] = (int.Parse(dailyShopDatas[2]) + 1).ToString();
                                        dailyShopData[int.Parse(index)] = string.Join("#", dailyShopDatas);
                                        dailyShop = string.Join("*", dailyShopData);
                                        ReturnData += await SetUserData(UserId, Characterdata_string.dailyShop, dailyShop);

                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} needType {needtype}");
                                    }
                                    else if (await CheckGoods(UserId, needtype, needvalue) == true)
                                    {
                                        // 구매 필요 재화 삭감
                                        ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                        // 보상
                                        string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                        string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                        string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                        string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                        string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                        string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                        string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                        string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                        if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                        if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                        if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                        if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);

                                        dailyShopDatas[2] = (int.Parse(dailyShopDatas[2]) + 1).ToString();
                                        dailyShopData[int.Parse(index)] = string.Join("#", dailyShopDatas);
                                        dailyShop = string.Join("*", dailyShopData);
                                        ReturnData += await SetUserData(UserId, Characterdata_string.dailyShop, dailyShop);

                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} needType {needtype}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.DailyShop_Reset:
                        {
                            string resetType = value1;

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.DailyShopReSet).ToString());

                            if (resetType == "1")
                            {
                                string curDailyShopResetAdCount = await GetUserData(UserId, Characterdata_int.DailyShopResetAdCount);
                                long curDailyShopResetAdCount_ = long.Parse(curDailyShopResetAdCount);

                                string AdTryMaxCount = systemdic[(int)systemtemplate_.value1];
                                long AdTryMaxCount_ = long.Parse(AdTryMaxCount);

                                if (curDailyShopResetAdCount_ < AdTryMaxCount_)
                                {
                                    ReturnData += await SetUserData(UserId, Characterdata_int.DailyShopResetAdCount, (curDailyShopResetAdCount_ + 1).ToString());
                                    ReturnData += await ResetDailyShop(UserId);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | resetType  {resetType}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"광고 보기 남은 횟수를 초과하였습니다.");
                                }
                            }
                            // 멜라 구매
                            else if (resetType == "2")
                            {
                                string curDailyShopResetMelaCount = await GetUserData(UserId, Characterdata_int.DailyShopResetMelaCount);
                                long curDailyShopResetMelaCount_ = long.Parse(curDailyShopResetMelaCount);

                                string resetMaxCount = systemdic[(int)systemtemplate_.value2];
                                long resetMaxCount_ = long.Parse(resetMaxCount);

                                if (curDailyShopResetMelaCount_ < resetMaxCount_)
                                {
                                    string needType = ((int)GoodsType_.Mela).ToString();
                                    string needValue = systemdic[(int)systemtemplate_.value3];

                                    if (await CheckGoods(UserId, needType, needValue) == true)
                                    {
                                        ReturnData += await SetUserData(UserId, Characterdata_int.DailyShopResetMelaCount, (curDailyShopResetMelaCount_ + 1).ToString());

                                        ReturnData += await RewardGoods(UserId, needType, needValue, false);
                                        ReturnData += await ResetDailyShop(UserId);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_ShopPopup, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | resetType  {resetType}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"초기화 횟수를 초과하였습니다.");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.User_Del:
                        {
                            await DelUserData(UserId);

                            ReturnData += MakePacket(ReturnPacket_.Update_UserDel, "");
                        }
                        break;
                    case Totalpacket_type.MineDailyShop_Buy:
                        {
                            string index = value1;

                            string dailyShop = await GetUserData(UserId, Characterdata_string.mineShop);
                            string[] dailyShopData = dailyShop.Split('*');
                            string[] dailyShopDatas = dailyShopData[int.Parse(index)].Split('#');

                            string templateIndex = dailyShopDatas[0];
                            string discountPer = dailyShopDatas[1];

                            string[] shopDic = await _redisService.GetTemplate(RedisService.TEMPLATE_TYPE.shop, templateIndex);

                            if (shopDic.Length > 0)
                            {
                                string maxcount = shopDic[(int)shoptemplate_.maxcount];
                                string needtype = shopDic[(int)shoptemplate_.needtype];
                                string needvalue = shopDic[(int)shoptemplate_.needvalue];

                                if (needvalue != "")
                                {
                                    long needvalue_ = long.Parse(needvalue);
                                    long discountPer_ = long.Parse(discountPer);
                                    needvalue = (needvalue_ - (needvalue_ * discountPer_ * 0.01f)).ToString();
                                }

                                if (int.Parse(dailyShopDatas[2]) >= int.Parse(maxcount))
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최대 구매 횟수입니다.");
                                }
                                else
                                {
                                    // 무료, 광고
                                    if (needtype == "0")
                                    {
                                        // 보상
                                        string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                        string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                        string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                        string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                        string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                        string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                        string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                        string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                        if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                        if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                        if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                        if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);

                                        dailyShopDatas[2] = (int.Parse(dailyShopDatas[2]) + 1).ToString();
                                        dailyShopData[int.Parse(index)] = string.Join("#", dailyShopDatas);
                                        dailyShop = string.Join("*", dailyShopData);
                                        ReturnData += await SetUserData(UserId, Characterdata_string.mineShop, dailyShop);

                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDailyShop, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} needType {needtype}");
                                    }
                                    else if (await CheckGoods(UserId, needtype, needvalue) == true)
                                    {
                                        // 구매 필요 재화 삭감
                                        ReturnData += await RewardGoods(UserId, needtype, needvalue, false);

                                        // 보상
                                        string rewardtype = shopDic[(int)shoptemplate_.rewardtype];
                                        string rewardvalue = shopDic[(int)shoptemplate_.rewardvalue];
                                        string rewardtype2 = shopDic[(int)shoptemplate_.rewardtype2];
                                        string rewardvalue2 = shopDic[(int)shoptemplate_.rewardvalue2];
                                        string rewardtype3 = shopDic[(int)shoptemplate_.rewardtype3];
                                        string rewardvalue3 = shopDic[(int)shoptemplate_.rewardvalue3];
                                        string rewardtype4 = shopDic[(int)shoptemplate_.rewardtype4];
                                        string rewardvalue4 = shopDic[(int)shoptemplate_.rewardvalue4];

                                        if (rewardtype.Length > 0 && rewardvalue.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype, rewardvalue);
                                        if (rewardtype2.Length > 0 && rewardvalue2.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype2, rewardvalue2);
                                        if (rewardtype3.Length > 0 && rewardvalue3.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype3, rewardvalue3);
                                        if (rewardtype4.Length > 0 && rewardvalue4.Length > 0) ReturnData += await RewardGoods(UserId, rewardtype4, rewardvalue4);

                                        dailyShopDatas[2] = (int.Parse(dailyShopDatas[2]) + 1).ToString();
                                        dailyShopData[int.Parse(index)] = string.Join("#", dailyShopDatas);
                                        dailyShop = string.Join("*", dailyShopData);
                                        ReturnData += await SetUserData(UserId, Characterdata_string.mineShop, dailyShop);

                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDailyShop, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} needType {needtype}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.MineDailyShop_Reset:
                        {
                            string resetType = value1;

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.DailyShopReSet).ToString());

                            if (resetType == "1")
                            {
                                string curDailyShopResetAdCount = await GetUserData(UserId, Characterdata_int.MineShopResetAdCount);
                                long curDailyShopResetAdCount_ = long.Parse(curDailyShopResetAdCount);

                                string AdTryMaxCount = systemdic[(int)systemtemplate_.value1];
                                long AdTryMaxCount_ = long.Parse(AdTryMaxCount);

                                if (curDailyShopResetAdCount_ < AdTryMaxCount_)
                                {
                                    ReturnData += await SetUserData(UserId, Characterdata_int.MineShopResetAdCount, (curDailyShopResetAdCount_ + 1).ToString());
                                    ReturnData += await ResetMineShop(UserId);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDailyShop, "");

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} | resetType  {resetType}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"광고 보기 남은 횟수를 초과하였습니다.");
                                }
                            }
                            // 멜라 구매
                            else if (resetType == "2")
                            {
                                string curDailyShopResetMelaCount = await GetUserData(UserId, Characterdata_int.MineShopResetMelaCount);
                                long curDailyShopResetMelaCount_ = long.Parse(curDailyShopResetMelaCount);

                                string resetMaxCount = systemdic[(int)systemtemplate_.value2];
                                long resetMaxCount_ = long.Parse(resetMaxCount);

                                if (curDailyShopResetMelaCount_ < resetMaxCount_)
                                {
                                    string needType = ((int)GoodsType_.Mela).ToString();
                                    string needValue = systemdic[(int)systemtemplate_.value3];

                                    if (await CheckGoods(UserId, needType, needValue) == true)
                                    {
                                        ReturnData += await SetUserData(UserId, Characterdata_int.MineShopResetMelaCount, (curDailyShopResetMelaCount_ + 1).ToString());

                                        ReturnData += await RewardGoods(UserId, needType, needValue, false);
                                        ReturnData += await ResetMineShop(UserId);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDailyShop, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_} | resetType  {resetType}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"초기화 횟수를 초과하였습니다.");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Bag_SetAether:
                        {
                            string invenIndex = value1;

                            string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                            string[] bagSlot_ = bagSlot.Split('#');

                            if (bagSlot_[5] == invenIndex || bagSlot_[6] == invenIndex)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 장착 중입니다.");
                            }
                            else
                            {
                                bool bHas = false;
                                string inven_aether = await GetUserData(UserId, Characterdata_string.inven_aether_data);

                                string[] inven_aether_data = inven_aether.Split('*');

                                for (int i = 0; i < inven_aether_data.Length; i++)
                                {
                                    string Index = inven_aether_data[i].Split('#')[0];
                                    if (Index == invenIndex) 
                                    {
                                        bHas = true;
                                        break;
                                    }
                                }

                                if (bHas) 
                                {
                                    long lv = long.Parse(await GetUserData(UserId, Characterdata_int.lv));

                                    int setIndex = 0;
                                    if (bagSlot_[5] == "0") setIndex = 5;
                                    else if (bagSlot_[6] == "0") setIndex = 6;

                                    if (setIndex > 0)
                                    {
                                        if (setIndex == 6 && lv < 20)
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "20레벨부터 장착 가능합니다.");
                                        }
                                        else
                                        {
                                            bagSlot_[setIndex] = invenIndex;
                                            bagSlot = string.Join("#", bagSlot_);

                                            ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "0");
                                        }
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "모든 에테르가 장착 중입니다.");
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "해당 에테르가 없습니다.");
                                }
                            }
                            
                        }
                        break;
                    case Totalpacket_type.Bag_UnsetAether:
                        {
                            string invenIndex = value1;

                            string bagSlot = await GetUserData(UserId, Characterdata_string.bagSlot);
                            string[] bagSlot_ = bagSlot.Split('#');

                            if (bagSlot_[5] != invenIndex && bagSlot_[6] != invenIndex)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "에테르 장착을 해제할 수 없습니다.");
                            }
                            else
                            {
                                int setIndex = 0;
                                if (bagSlot_[5] == invenIndex) setIndex = 5;
                                else if (bagSlot_[6] == invenIndex) setIndex = 6;

                                if (setIndex > 0) 
                                {
                                    bagSlot_[setIndex] = "0";
                                    bagSlot = string.Join("#", bagSlot_);

                                    ReturnData += await SetUserData(UserId, Characterdata_string.bagSlot, bagSlot);
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_State, "");
                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Bag_Inven, "0");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "에테르 장착을 해제할 수 없습니다.");
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.MineEnergyCheck:
                        {
                            ReturnData += await CheckMineEnergy(UserId);
                        }
                        break;
                    case Totalpacket_type.MineingStart:
                        {
                            string autoStart = value1;

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Mine).ToString());
                            string needValue = systemdic[(int)systemtemplate_.value2];
                            string givenTime = systemdic[(int)systemtemplate_.value3];

                            string mineEnergy = await GetUserData(UserId, Characterdata_int.MineEnergy);
                            string mineMaxEnergy = await GetUserData(UserId, Characterdata_int.MineMaxEnergy);
                            string mineState = await GetUserData(UserId, Characterdata_int.MineState);

                            int mineEnergy_ = int.Parse(mineEnergy);
                            int needValue_ = int.Parse(needValue);
                            int givenTime_ = int.Parse(givenTime);

                            if (mineState == "0")
                            {
                                string mineGivenTime = await GetUserData(UserId, Characterdata_string.mineGivenTime);

                                if (DateTime.Parse(mineGivenTime) < DateTime.Now) 
                                {
                                    if (autoStart == "0")
                                    {
                                        if (mineEnergy_ >= needValue_)
                                        {
                                            DateTime now = DateTime.Now;

                                            ReturnData += await RewardGoods(UserId, ((int)GoodsType_.MineEnergy).ToString(), needValue, false);
                                            ReturnData += await SetUserData(UserId, Characterdata_string.mineGivenTime, (now.AddMinutes(givenTime_).ToString("yyyy-MM-dd HH:mm:ss")));

                                            ReturnData += await SetUserData(UserId, Characterdata_int.MineState, "1");

                                            if (mineMaxEnergy == mineEnergy) ReturnData += await SetUserData(UserId, Characterdata_string.lastMineEnergyGivenTime, _redisService.GetDateTimeNow());
                                            WriteLog(UserId, $"{(Totalpacket_type)type_} autoStart {autoStart}");
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "채굴 가능한 기력이 부족합니다.");
                                        }
                                    }
                                    else if (autoStart == "1")
                                    {
                                        string[] mineSystemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.Mine).ToString());

                                        string autoMineMaxCount = mineSystemdic[(int)systemtemplate_.value4];

                                        string mineProficiency = await GetUserData(UserId, Characterdata_int.MineProficiency);
                                        string autoMineAdCount = await GetUserData(UserId, Characterdata_int.AutoMineAdCount);

                                        int mineProficiency_ = int.Parse(mineProficiency);
                                        int autoMineAdCount_ = int.Parse(autoMineAdCount);
                                        int autoMineMaxCount_ = int.Parse(autoMineMaxCount);

                                        if (autoMineAdCount_ < autoMineMaxCount_) 
                                        {
                                            if (mineEnergy_ >= needValue_)
                                            {
                                                int rewardCount = mineEnergy_ / needValue_;

                                                string mineReward = await GetUserData(UserId, Characterdata_string.mineReward);
                                                string[] mineRewardSystemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineReward).ToString());

                                                string[] rewardPer = mineRewardSystemdic[(int)systemtemplate_.value1].Split('#');
                                                string[] rewardType = mineRewardSystemdic[(int)systemtemplate_.value2].Split('#');
                                                string[] rewardValue = mineRewardSystemdic[(int)systemtemplate_.value3].Split('#');


                                                for (int i = 0; i < rewardCount; i++)
                                                {
                                                    float totalPer = 0;
                                                    for (int j = 0; j < rewardPer.Length; j++) totalPer += float.Parse(rewardPer[j]);

                                                    float rand = GetRandomFloat(0, totalPer);
                                                    float cumulative = 0f;

                                                    for (int j = 0; j < rewardPer.Length; j++)
                                                    {
                                                        cumulative += float.Parse(rewardPer[j]);
                                                        if (rand < cumulative)
                                                        {
                                                            if (rewardType[j] != "0" && rewardValue[j] != "0")
                                                            {
                                                                if (mineReward == "") mineReward = $"{rewardType[j]}#{rewardValue[j]}";
                                                                else mineReward = $"{mineReward}*{rewardType[j]}#{rewardValue[j]}";
                                                            }

                                                            break;
                                                        }
                                                    }

                                                    mineProficiency_++;
                                                }
                                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.MineEnergy).ToString(), (rewardCount * needValue_).ToString(), false);
                                                ReturnData += await SetUserData(UserId, Characterdata_string.mineGivenTime, _redisService.GetDateTimeNow());
                                                ReturnData += await SetUserData(UserId, Characterdata_string.mineReward, mineReward);
                                                ReturnData += await SetUserData(UserId, Characterdata_int.MineProficiency, mineProficiency_.ToString());
                                                ReturnData += await SetUserData(UserId, Characterdata_int.AutoMineAdCount, (autoMineAdCount_ + 1).ToString());

                                                if (mineMaxEnergy == mineEnergy) ReturnData += await SetUserData(UserId, Characterdata_string.lastMineEnergyGivenTime, _redisService.GetDateTimeNow());
                                                WriteLog(UserId, $"{(Totalpacket_type)type_} autoStart {autoStart}");
                                            }
                                            else
                                            {
                                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "채굴 가능한 기력이 부족합니다.");
                                            }
                                        }
                                        else
                                        {
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"광고 보기 남은 횟수를 초과하였습니다.");
                                        }
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "채굴이 진행 중 입니다.");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "채굴이 진행 중 입니다.");
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePick, "");
                        }
                        break;
                    case Totalpacket_type.MineingCheck:
                        {
                            ReturnData += await CheckMineing(UserId);
                        }
                        break;
                    case Totalpacket_type.MineReward:
                        {
                            string curMineReward = await GetUserData(UserId, Characterdata_string.mineReward);

                            if (curMineReward != "") 
                            {
                                string[] curMineRewards = curMineReward.Split('*');

                                for (int i = 0; i < curMineRewards.Length; i++)
                                {
                                    string[] rewardData = curMineRewards[i].Split('#');
                                    string rewardType = rewardData[0];
                                    string rewardValue = rewardData[1];

                                    ReturnData += await RewardGoods(UserId, rewardType, rewardValue);
                                }

                                ReturnData += await SetUserData(UserId, Characterdata_string.mineReward, "");

                                WriteLog(UserId, $"{(Totalpacket_type)type_} curMineReward {curMineReward}");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "보상을 보두 수령했습니다.");
                            }
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                        }
                        break;
                    case Totalpacket_type.MineDigUpgrade:
                        {
                            string curMineDigLv = await GetUserData(UserId, Characterdata_int.MineDigLv);
                            int curMineDigLv_ = int.Parse(curMineDigLv);

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDigLv).ToString());
                            string digMaxLv = systemdic[(int)systemtemplate_.value1];

                            if (curMineDigLv_ >= int.Parse(digMaxLv))
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최고 레벨입니다.");
                            }
                            else
                            {
                                string nextNeedData = systemdic[(int)systemtemplate_.value1 + curMineDigLv_];

                                if (nextNeedData != "") 
                                {
                                    string[] nextNeedDatas = nextNeedData.Split('^');
                                    string[] nextNeedRewards = nextNeedDatas[1].Split('*');

                                    bool bUpgrade = true;
                                    for (int i = 0; i < nextNeedRewards.Length; i++)
                                    {
                                        string[] nextNeedRewards_ = nextNeedRewards[i].Split('#');

                                        string needtype = nextNeedRewards_[0];
                                        string needvalue = nextNeedRewards_[1];

                                        if(await CheckGoods(UserId, needtype, needvalue) == false)
                                        {
                                            bUpgrade = false;
                                            break;
                                        }
                                    }

                                    if (bUpgrade)
                                    {
                                        for (int i = 0; i < nextNeedRewards.Length; i++)
                                        {
                                            string[] nextNeedRewards_ = nextNeedRewards[i].Split('#');

                                            string needtype = nextNeedRewards_[0];
                                            string needvalue = nextNeedRewards_[1];

                                            ReturnData += await RewardGoods(UserId, needtype, needvalue, false);
                                        }
                                        ReturnData += await SetUserData(UserId, Characterdata_int.MineMaxEnergy, nextNeedDatas[0]);
                                        ReturnData += await SetUserData(UserId, Characterdata_int.MineDigLv, (curMineDigLv_ + 1).ToString());
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDig, "");
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");

                                        WriteLog(UserId, $"{(Totalpacket_type)type_}");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                            }

                        }
                        break;
                    case Totalpacket_type.MineDefense_Set:
                        {
                            string setIndex = value1;
                            string setGoodsType = value2;

                            int setIndex_ = int.Parse(setIndex);
                            int setGoodsType_ = int.Parse(setGoodsType);

                            string mineDefense = await GetUserData(UserId, Characterdata_string.mineDefense);
                            string[] mineDefenses = mineDefense.Split('#');

                            if (setGoodsType_ > 0)
                            {
                                if (mineDefenses[setIndex_] != setGoodsType) 
                                {
                                    if (await CheckGoods(UserId, setGoodsType, "1") == true)
                                    {
                                        ReturnData += await RewardGoods(UserId, setGoodsType, "1", false);

                                        if (mineDefenses[setIndex_] != "0") ReturnData += await RewardGoods(UserId, mineDefenses[setIndex_], "1", true, false);

                                        mineDefenses[setIndex_] = setGoodsType;
                                        ReturnData += await SetUserData(UserId, Characterdata_string.mineDefense, string.Join("#", mineDefenses));
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                                else if (mineDefenses[setIndex_] == setGoodsType)
                                {
                                    ReturnData += await RewardGoods(UserId, mineDefenses[setIndex_], "1", true, false);
                                    mineDefenses[setIndex_] = "0";
                                    ReturnData += await SetUserData(UserId, Characterdata_string.mineDefense, string.Join("#", mineDefenses));
                                }
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDefense, "");
                        }
                        break;
                    case Totalpacket_type.MineDefense_Rune:
                        {
                            string setIndex = value1;
                            string setGoodsType = value2;

                            int setIndex_ = int.Parse(setIndex);

                            string mineRune = await GetUserData(UserId, Characterdata_string.mineDefense_Rune);
                            string[] mineRunes = mineRune.Split('#');

                            int curRuneLv = int.Parse(mineRunes[setIndex_]);

                            if(curRuneLv >= 10)
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "최고 레벨입니다.");
                            }
                            else
                            {
                                string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDefense_Rune).ToString());
                                string[] lvUpNeedValue = systemdic[(int)systemtemplate_.value1].Split('#');
                                string[] roomNeedValue = systemdic[(int)systemtemplate_.value2].Split('#');

                                if (await CheckGoods(UserId, setGoodsType, lvUpNeedValue[curRuneLv]) == true &&
                                    await CheckGoods(UserId, ((int)GoodsType_.Room).ToString(), roomNeedValue[curRuneLv]) == true)
                                {
                                    ReturnData += await RewardGoods(UserId, setGoodsType, lvUpNeedValue[curRuneLv], false);
                                    ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Room).ToString(), roomNeedValue[curRuneLv], false);

                                    mineRunes[setIndex_] = (curRuneLv + 1).ToString();
                                    ReturnData += await SetUserData(UserId, Characterdata_string.mineDefense_Rune, string.Join("#", mineRunes));
                                    ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.effect_getskill).ToString());

                                    WriteLog(UserId, $"{(Totalpacket_type)type_} setIndex {setIndex} setGoodsType {setGoodsType}");
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                }
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MineDefense, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                        }
                        break;
                    case Totalpacket_type.MineAmalgamation_Make_Defense:
                        {
                            string tabType = value1;
                            string setGoodsType = value2;

                            int setGoodsType_ = int.Parse(setGoodsType);

                            string needStoneType = "";
                            string needStone = "";
                            string needRoom = "";
                            string needColdBreath = "";

                            switch (int.Parse(tabType))
                            {
                                case (int)RedisService.AmalgamationType_.MagicBullet:
                                    {
                                        needStoneType = ((int)GoodsType_.Stone_Ore_Mana).ToString();
                                        string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDefense_MagicBullet).ToString());
                                        needStone = systemdic[(int)systemtemplate_.value1].Split('#')[setGoodsType_ - (int)GoodsType_.Stone_Ore_Magicbullet_Low];
                                        needRoom = systemdic[(int)systemtemplate_.value2].Split('#')[setGoodsType_ - (int)GoodsType_.Stone_Ore_Magicbullet_Low];
                                        needColdBreath = systemdic[(int)systemtemplate_.value3].Split('#')[setGoodsType_ - (int)GoodsType_.Stone_Ore_Magicbullet_Low];
                                    }
                                    break;
                                case (int)RedisService.AmalgamationType_.Barrier:
                                    {
                                        needStoneType = ((int)GoodsType_.Stone_Ore_Barrier).ToString();
                                        string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDefense_Barrier).ToString());
                                        needStone = systemdic[(int)systemtemplate_.value1].Split('#')[setGoodsType_ - (int)GoodsType_.Stone_Ore_Barrier_Low];
                                        needRoom = systemdic[(int)systemtemplate_.value2].Split('#')[setGoodsType_ - (int)GoodsType_.Stone_Ore_Barrier_Low];
                                        needColdBreath = systemdic[(int)systemtemplate_.value3].Split('#')[setGoodsType_ - (int)GoodsType_.Stone_Ore_Barrier_Low];
                                    }
                                    break;
                            }

                            if(needColdBreath == "0" && await CheckGoods(UserId, needStoneType, needStone) == true &&
                                await CheckGoods(UserId, ((int)GoodsType_.Room).ToString(), needRoom) == true)
                            {
                                ReturnData += await RewardGoods(UserId, needStoneType, needStone, false);
                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Room).ToString(), needRoom, false);
                                ReturnData += await RewardGoods(UserId, setGoodsType, "1");

                                WriteLog(UserId, $"{(Totalpacket_type)type_} tabType {tabType} setGoodsType {setGoodsType}");
                            }
                            else if (needColdBreath != "0" && await CheckGoods(UserId, needStoneType, needStone) == true &&
                                await CheckGoods(UserId, ((int)GoodsType_.Room).ToString(), needRoom) == true &&
                                await CheckGoods(UserId, ((int)GoodsType_.ColdBreath).ToString(), needColdBreath) == true)
                            {
                                ReturnData += await RewardGoods(UserId, needStoneType, needStone, false);
                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Room).ToString(), needRoom, false);
                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.ColdBreath).ToString(), needColdBreath, false);
                                ReturnData += await RewardGoods(UserId, setGoodsType, "1");

                                WriteLog(UserId, $"{(Totalpacket_type)type_} tabType {tabType} setGoodsType {setGoodsType}");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Amalgamation_Tab, tabType);
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Amalgamation_Defense, setGoodsType);
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                        }
                        break;
                    case Totalpacket_type.MineAmalgamation_Make_Aether:
                        {
                            string abilityType = value1;
                            string aetherInvenIndex = value2;

                            if (aetherInvenIndex != "") 
                            {
                                int abilityType_ = int.Parse(abilityType);
                                int addIndex = abilityType_ - (int)Abilitytype_.Aether_Fire;
                                string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineAmalgamation_AetherAbililty).ToString());
                                string abilityDefaultValue = systemdic[(int)systemtemplate_.value1 + addIndex];

                                string inven_aether = await GetUserData(UserId, Characterdata_string.inven_aether_data);
                                string[] inven_aether_data = inven_aether.Split('*');

                                if (await CheckGoods(UserId, ((int)GoodsType_.Stone_Ore_Fire + addIndex).ToString(), "5") == true &&
                                    await CheckGoods(UserId, ((int)GoodsType_.Room).ToString(), "50000") == true)
                                {
                                    for (int i = 0; i < inven_aether_data.Length; i++)
                                    {
                                        string[] inven_aether_datas = inven_aether_data[i].Split('#');
                                        string Index = inven_aether_datas[(int)User_Aether.invenIndex];
                                        if (Index == aetherInvenIndex)
                                        {
                                            if (inven_aether_datas[(int)User_Aether.addAbilityType] == "" && inven_aether_datas[(int)User_Aether.addAbilityValue] == "")
                                            {
                                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Stone_Ore_Fire + addIndex).ToString(), "5", false);
                                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Room).ToString(), "50000", false);

                                                inven_aether_datas[(int)User_Aether.addAbilityType] = abilityType;
                                                inven_aether_datas[(int)User_Aether.addAbilityValue] = abilityDefaultValue;

                                                inven_aether_data[i] = string.Join("#", inven_aether_datas);

                                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenAether, inven_aether_data[i], aetherInvenIndex);
                                                ReturnData += await SetUserData(UserId, Characterdata_string.inven_aether_data, string.Join("*", inven_aether_data));

                                                string desc = await GetAbilityDesc(abilityType, abilityDefaultValue);
                                                string icon = await GetAbilityIcon(abilityType);
                                                string name = await GetAbilityName(abilityType);

                                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_RewardPopup, "", icon, name, desc, "", "", "", "융합로");
                                                ReturnData += await OnQuest(UserId, QuestIndex_.AmalgamationAether, 1);

                                                WriteLog(UserId, $"{(Totalpacket_type)type_} abilityType {abilityType} aetherInvenIndex {aetherInvenIndex}");
                                            }
                                            else
                                            {
                                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 융합된 에테르입니다.");
                                            }
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "융합할 에테르가 없습니다.");
                            }

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Amalgamation_Tab, "0");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Amalgamation_Aether, abilityType);
                        }
                        break;
                    case Totalpacket_type.MineAmalgamation_Change_Esa:
                        {
                            string tabType = value1;
                            string aetherIndex = value2;
                            string petIndex = value3;

                            string[] aetherIndexs = aetherIndex.Split('#');
                            string[] petIndexs = petIndex.Split('#');

                            string inven_aether = await GetUserData(UserId, Characterdata_string.inven_aether_data);
                            string[] inven_aether_data = inven_aether.Split('*');

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDefense_EsaChange).ToString());

                            Dictionary<string, string> inven_aether_Dic = new Dictionary<string, string>();
                            for (int i = 0; i < inven_aether_data.Length; i++)
                            {
                                if (inven_aether_data[i] == "") continue;

                                string[] inven_aether_datas = inven_aether_data[i].Split('#');

                                if (inven_aether_Dic.ContainsKey(inven_aether_datas[(int)User_Aether.invenIndex]) == false) inven_aether_Dic.Add(inven_aether_datas[(int)User_Aether.invenIndex], inven_aether_data[i]);
                                else inven_aether_Dic[inven_aether_datas[(int)User_Aether.invenIndex]] = inven_aether_data[i];
                            }

                            int esaCount = 0;

                            for (int i = 0; i < aetherIndexs.Length; i++)
                            {
                                if (aetherIndexs[i] == "") continue;

                                if (inven_aether_Dic.ContainsKey(aetherIndexs[i])) 
                                {
                                    string[] inven_aether_datas = inven_aether_Dic[aetherIndexs[i]].Split('#');

                                    if (inven_aether_datas[(int)User_Aether.addAbilityType] == "" &&
                                        inven_aether_datas[(int)User_Aether.addAbilityValue] == "")
                                    {
                                        esaCount += int.Parse(systemdic[(int)systemtemplate_.value1]);
                                    }
                                    else
                                    {
                                        esaCount += int.Parse(systemdic[(int)systemtemplate_.value2]);
                                    }

                                    inven_aether_Dic.Remove(aetherIndexs[i]);
                                    ReturnData += MakePacket(ReturnPacket_.Update_UserInvenAether, "", aetherIndexs[i]);
                                }
                            }

                            ReturnData += await SetUserData(UserId, Characterdata_string.inven_aether_data, string.Join("*", inven_aether_Dic.Values));

                            string[] addEsaCount_Pet = systemdic[(int)systemtemplate_.value3].Split('#');

                            for (int i = 0; i < petIndexs.Length; i++)
                            {
                                if (petIndexs[i] == "") continue;

                                string petInvenData = await _redisService.GetPetInven(UserId, petIndexs[i]);
                                if (petInvenData != "")
                                {
                                    string[] petInvenDatas = petInvenData.Split('#');

                                    string grade = petInvenDatas[(int)User_Pet.grade];

                                    esaCount += int.Parse(addEsaCount_Pet[int.Parse(grade)]);

                                    await _redisService.DelPetInven(UserId, petIndexs[i]);
                                    ReturnData += MakePacket(ReturnPacket_.Update_UserInvenPet, "", petIndexs[i]);
                                }
                            }

                            WriteLog(UserId, $"{(Totalpacket_type)type_} AddAsaCount {esaCount}");

                            ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Esa).ToString(), esaCount.ToString());
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_MinePopup, "");
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Amalgamation_EsaChange, tabType);
                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_Goods, "");
                        }
                        break;
                    case Totalpacket_type.Mine_Aether_Upgrade:
                        {
                            string aetherInvenIndex = value1;
                            string scrollType = value2;
                            string addUpgradeCount = value3;

                            int addUpgradeCount_ = int.Parse(addUpgradeCount);

                            if (aetherInvenIndex != "" && scrollType != "")
                            {
                                string starFire = await GetUserData(UserId, Characterdata_int.Material_StarFire);
                                int starFire_ = int.Parse(starFire);

                                string inven_aether = await GetUserData(UserId, Characterdata_string.inven_aether_data);
                                string[] inven_aether_data = inven_aether.Split('*');

                                bool bPossible = true;
                                if (addUpgradeCount_ > 0)
                                {
                                    if(starFire_ < addUpgradeCount_)
                                    {
                                        bPossible = false;
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "별의 불씨가 부족합니다.");
                                    }
                                }

                                for (int i = 0; i < inven_aether_data.Length; i++)
                                {
                                    string[] inven_aether_datas = inven_aether_data[i].Split('#');
                                    string Index = inven_aether_datas[(int)User_Aether.invenIndex];

                                    if (Index == aetherInvenIndex)
                                    {
                                        if(inven_aether_datas[(int)User_Aether.addAbilityType] == "" || inven_aether_datas[(int)User_Aether.addAbilityValue] == "")
                                        {
                                            bPossible = false;
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "융합된 에테르만 강화가 가능합니다.");
                                        }

                                        if (inven_aether_datas[(int)User_Aether.upgradeCount] != "" &&
                                            int.Parse(inven_aether_datas[(int)User_Aether.upgradeCount]) >= 5)
                                        {
                                            bPossible = false;
                                            ReturnData += MakePacket(ReturnPacket_.ToastDesc, "더이상 업그레이드 횟수가 남지 않았습니다.");
                                        }
                                        break;
                                    }
                                }

                                if (await CheckGoods(UserId, scrollType, "1") == true)
                                {
                                    if (bPossible)
                                    {
                                        if (addUpgradeCount_ > 0) ReturnData += await RewardGoods(UserId, ((int)GoodsType_.Material_StarFire).ToString(), addUpgradeCount, false);
                                        ReturnData += await RewardGoods(UserId, scrollType, "1", false);

                                        int scrollType_ = int.Parse(scrollType);

                                        int per = 0;
                                        int addAbilityValue = 0;
                                        switch (scrollType_)
                                        {
                                            case (int)GoodsType_.Aether_Upgrade_Scroll_80:
                                                {
                                                    per = 80;
                                                    addAbilityValue = 4;
                                                }
                                                break;
                                            case (int)GoodsType_.Aether_Upgrade_Scroll_40:
                                                {
                                                    per = 40;
                                                    addAbilityValue = 8;
                                                }
                                                break;
                                            case (int)GoodsType_.Aether_Upgrade_Scroll_10:
                                                {
                                                    per = 10;
                                                    addAbilityValue = 15;
                                                }
                                                break;
                                        }

                                        per += addUpgradeCount_ * 2;

                                        int ran = _random.Next(0, 100);

                                        for (int i = 0; i < inven_aether_data.Length; i++)
                                        {
                                            string[] inven_aether_datas = inven_aether_data[i].Split('#');
                                            string Index = inven_aether_datas[(int)User_Aether.invenIndex];

                                            if (Index == aetherInvenIndex)
                                            {
                                                if (ran < per)
                                                {
                                                    if (inven_aether_datas[(int)User_Aether.upgradeCount] == "") inven_aether_datas[(int)User_Aether.upgradeCount] = "1";
                                                    else inven_aether_datas[(int)User_Aether.upgradeCount] = (int.Parse(inven_aether_datas[(int)User_Aether.upgradeCount]) + 1).ToString();

                                                    inven_aether_datas[(int)User_Aether.upgradeAbilityType] = inven_aether_datas[(int)User_Aether.addAbilityType];

                                                    if (inven_aether_datas[(int)User_Aether.upgradeAbilityValue] == "") inven_aether_datas[(int)User_Aether.upgradeAbilityValue] = addAbilityValue.ToString();
                                                    else inven_aether_datas[(int)User_Aether.upgradeAbilityValue] = (int.Parse(inven_aether_datas[(int)User_Aether.upgradeAbilityValue]) + addAbilityValue).ToString();

                                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "강화을 성공했습니다.");
                                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Aether_Upgrade_Success, "");
                                                    ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.effect_aeather_upgrade_success).ToString());

                                                    WriteLog(UserId, $"{(Totalpacket_type)type_} Upgrade Succes scrollType_ {scrollType_}");
                                                }
                                                else
                                                {
                                                    if (inven_aether_datas[(int)User_Aether.upgradeCount] == "") inven_aether_datas[(int)User_Aether.upgradeCount] = "1";
                                                    else inven_aether_datas[(int)User_Aether.upgradeCount] = (int.Parse(inven_aether_datas[(int)User_Aether.upgradeCount]) + 1).ToString();

                                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "강화에 실패했습니다.");
                                                    ReturnData += MakePacket(ReturnPacket_.UpdateUI_Aether_Upgrade_Fail, "");
                                                    ReturnData += MakePacket(ReturnPacket_.Update_SoundIndex, ((int)SoundIndex_.effect_aether_upgrade_fail).ToString());

                                                    WriteLog(UserId, $"{(Totalpacket_type)type_} Upgrade Fail scrollType_ {scrollType_}");
                                                }

                                                inven_aether_data[i] = string.Join("#", inven_aether_datas);

                                                ReturnData += MakePacket(ReturnPacket_.Update_UserInvenAether, inven_aether_data[i], aetherInvenIndex);
                                                ReturnData += await SetUserData(UserId, Characterdata_string.inven_aether_data, string.Join("*", inven_aether_data));
                                                break;
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, "주문서가 부족합니다.");
                                }
                                
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Aether_Upgrade, "");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "융합 에테르와 주문서가 선택되지 않았습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.MineDefenseBattleCheck:
                        {
                            string adTry = value1;

                            string mineDefense = await GetUserData(UserId, Characterdata_string.mineDefense);
                            string[] mineDefenses = mineDefense.Split('#');

                            if (mineDefenses[0] == "0" && mineDefenses[1] == "0" || mineDefenses[2] == "0")
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, "미사일 또는 결계 장착이 필요합니다.");
                            }
                            else
                            {
                                if (adTry == "0")
                                {
                                    if (await CheckGoods(UserId, ((int)GoodsType_.MineDefensePlayCount).ToString(), "1") == true)
                                    {
                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.MineDefensePlayCount).ToString(), "1", false);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_DefenseBattleStart, "");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                                else if (adTry == "1")
                                {
                                    string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDefenseBattleDailyCount).ToString());
                                    string adMaxCount = systemdic[(int)systemtemplate_.value2];

                                    string mineDefensePlayAdTry = await GetUserData(UserId, Characterdata_int.MineDefensePlayAdTry);
                                    int mineDefensePlayAdTry_ = int.Parse(mineDefensePlayAdTry);
                                    int adMaxCount_ = int.Parse(adMaxCount);

                                    if (mineDefensePlayAdTry_ < adMaxCount_)
                                    {
                                        ReturnData += await SetUserData(UserId, Characterdata_int.MineDefensePlayAdTry, (mineDefensePlayAdTry_ + 1).ToString());
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_DefenseBattleStart, "");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"광고 보기 남은 횟수를 초과하였습니다.");
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.MineDefenseBattleSetRemove:
                        {
                            string mineDefense = await GetUserData(UserId, Characterdata_string.mineDefense);
                            string[] mineDefenses = mineDefense.Split('#');

                            mineDefenses[0] = "0";
                            mineDefenses[1] = "0";
                            mineDefenses[2] = "0";

                            ReturnData += await SetUserData(UserId, Characterdata_string .mineDefense, string.Join("#", mineDefenses));
                        }
                        break;
                    case Totalpacket_type.MineDefenseBattleReward:
                        {
                            string stage = value1;
                            int stage_ = int.Parse(stage);

                            ReturnData += MakePacket(ReturnPacket_.UpdateUI_DefenseBattleReward, "");

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.MineDefenseBattleReward).ToString());
                            string rewardData = systemdic[(int)systemtemplate_.value1 + stage_ - 1];

                            string[] rewardDatas = rewardData.Split('*');
                            string[] rewardPer = rewardDatas[0].Split('#');
                            string[] rewardType = rewardDatas[1].Split('#');
                            string[] rewardValue = rewardDatas[2].Split('#');

                            float totalPer = 0;
                            for (int i = 0; i < rewardPer.Length; i++) totalPer += float.Parse(rewardPer[i]);

                            for (int i = 0; i < 3; i++)
                            {
                                float rand = GetRandomFloat(0, totalPer);
                                float cumulative = 0f;

                                for (int j = 0; j < rewardPer.Length; j++)
                                {
                                    cumulative += float.Parse(rewardPer[j]);
                                    if (rand < cumulative)
                                    {
                                        if (rewardType[j] != "0" && rewardValue[j] != "0")
                                        {
                                            ReturnData += await RewardGoods(UserId, rewardType[j], rewardValue[j]);
                                        }

                                        break;
                                    }
                                }
                            }
                        }
                        break;
                    case Totalpacket_type.Skin_Set:
                        {
                            string setId = value1;

                            string inven_skin = await GetUserData(UserId, RedisService.Characterdata_string.inven_skin_data);
                            string[] inven_skin_data = inven_skin.Split('*');

                            bool bHas = false;
                            for (int i = 0; i < inven_skin_data.Length; i++)
                            {
                                if (inven_skin_data[i] == "") continue;

                                string[] inven_skin_datas = inven_skin_data[i].Split('#');

                                string id = inven_skin_datas[(int)User_Skin.id];
                                string count = inven_skin_datas[(int)User_Skin.count];

                                if(id == setId)
                                {
                                    bHas = true;
                                    break;
                                }
                            }

                            if (bHas) 
                            {
                                string skinSlot = await GetUserData(UserId, Characterdata_string.skinSlot);
                                string[] skinSlot_ = skinSlot.Split('#');

                                if (skinSlot_[0] == setId)
                                {
                                    skinSlot_[0] = "0";
                                }
                                else if (skinSlot_[1] == setId) 
                                {
                                    skinSlot_[1] = "0";
                                }
                                else
                                {
                                    string[] skinDic = await _redisService.GetTemplate_Indexing(TEMPLATE_TYPE.skin, setId);
                                    string skinType = skinDic[(int)skintemplate_.type];

                                    if (skinType == "2") skinSlot_[0] = setId;
                                    else if (skinType == "3") skinSlot_[1] = setId;
                                }
                                ReturnData += await SetUserData(UserId, Characterdata_string.skinSlot, string.Join("#", skinSlot_));
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Skin, "");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_Character, "");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_CharacterIcon, "");
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"해당 스킨을 가지고 있지 않습니다.");
                            }
                        }
                        break;
                    case Totalpacket_type.EventGameCheck:
                        {
                            string state = value1;

                            if (state == "0")
                            {
                                string eventEndTime = await GetUserData(UserId, RedisService.Characterdata_string.EventEndTime);

                                if (eventEndTime != "" && DateTime.Now < DateTime.Parse(eventEndTime))
                                {
                                    if (await CheckGoods(UserId, ((int)GoodsType_.SnowBall).ToString(), "5") == true)
                                    {
                                        ReturnData += await RewardGoods(UserId, ((int)GoodsType_.SnowBall).ToString(), "5", false);
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventGameStart, "");
                                    }
                                    else
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "재화가 부족합니다.");
                                    }
                                }
                                else
                                {
                                    ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"이벤트 기간이 아닙니다.");
                                }
                            }
                            else if (state == "1")
                            {
                                string score = value2;

                                ReturnData += await SetUserData(UserId, Characterdata_int.EventSnowFlowerPlusPer, "0");
                                ReturnData += await RewardGoods(UserId, ((int)GoodsType_.SnowFlower).ToString(), score);
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventPopup, "");
                            }
                            else if (state == "2")
                            {
                                ReturnData += await SetUserData(UserId, Characterdata_int.EventSnowFlowerPlusPer, "1");
                                ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventPopup, "");
                            }
                        }
                        break;
                    case Totalpacket_type.EventRewardCheck:
                        {
                            string rewardIndex = value1;
                            int rewardIndex_ = int.Parse(rewardIndex);

                            string[] systemdic = await _redisService.GetTemplate(TEMPLATE_TYPE.systemdata, ((int)systemdataindex_.EventReward).ToString());

                            string[] rewardSocre = systemdic[(int)systemtemplate_.value1].Split('#');
                            string[] rewardData = systemdic[(int)systemtemplate_.value2].Split('*');
                            string[] curReward = (await GetUserData(UserId, Characterdata_string.EventReward)).Split('#');
                            string snowFlower = await GetUserData(UserId, Characterdata_int.SnowFlower);
                            int snowFlower_ = int.Parse(snowFlower);

                            if (snowFlower_ >= int.Parse(rewardSocre[rewardIndex_])) 
                            {
                                if (curReward.Length > rewardIndex_) 
                                {
                                    if (curReward[rewardIndex_] == "0")
                                    {
                                        string[] rewardDatas = rewardData[rewardIndex_].Split('#');

                                        ReturnData += await RewardGoods(UserId, rewardDatas[0], rewardDatas[1]);
                                        curReward[rewardIndex_] = "1";
                                        ReturnData += await SetUserData(UserId, Characterdata_string.EventReward, string.Join("#", curReward));
                                        ReturnData += MakePacket(ReturnPacket_.UpdateUI_EventReward, "");
                                    }
                                    else if (curReward[rewardIndex_] == "1")
                                    {
                                        ReturnData += MakePacket(ReturnPacket_.ToastDesc, "이미 보상을 수령하였습니다.");
                                    }
                                }
                            }
                            else
                            {
                                ReturnData += MakePacket(ReturnPacket_.ToastDesc, $"눈꽃 점수가 필요합니다.");
                            }
                        }
                        break;


                    //////////////////////////////////////패킷은 이 위로
                    case Totalpacket_type.Cheatkey:
                        {
                            string cheatType = value1;

                            if (cheatType == "") break;

                            if (StoreType == "none") 
                            {
                                if (cheatType.Contains("cheat"))
                                {
                                    string[] cheatTypes = cheatType.Split(' ');
                                    string rewardType = cheatTypes[1];
                                    string rewardValue = cheatTypes[2];

                                    await _redisService.ProcessPost(UserId, rewardType, rewardValue, "치트 보상");
                                }
                                else
                                {
                                    switch (int.Parse(cheatType))
                                    {
                                        case (int)CheatType_.Post:
                                            {
                                                int ranrewardtype = _random.Next(1, (int)GoodsType_.Exp);
                                                ranrewardtype = 103;
                                                int ranrewardvalue = _random.Next(2, 5);
                                                string[] desc = new string[] { "패치 점검 보상", "점검 보상", "리워드 보상", "수환쓰 보상", "승환쓰 보상" };
                                                int randesc = _random.Next(0, desc.Length);
                                                int expireday = 10;

                                                await _redisService.ProcessPost(UserId, ranrewardtype.ToString(), ranrewardvalue.ToString(), desc[randesc], expireday.ToString());

                                                ranrewardtype = 3;
                                                ranrewardvalue = 10000;

                                                await _redisService.ProcessPost(UserId, ranrewardtype.ToString(), ranrewardvalue.ToString(), desc[randesc], expireday.ToString());
                                                await _redisService.ProcessPost(UserId, "1", "10000", desc[randesc], expireday.ToString());
                                                await _redisService.ProcessPost(UserId, "2", "10000", desc[randesc], expireday.ToString());


                                            }
                                            break;
                                        case (int)CheatType_.Notice:
                                            {
                                                string notice = await _redisService.GetNotice();

                                                int rannum = _random.Next(1, 100);

                                                string msg = "관리자 노트 : " + rannum + " 버전 업데이트";

                                                if (notice == "") notice = msg;
                                                else notice = msg + "#" + notice;

                                                await _redisService.SetNotice(notice);
                                                await _redisService.SetNoticeVersion();
                                            }
                                            break;
                                        case (int)CheatType_.HeroSlot:
                                            {
                                                ReturnData += await SetUserData(UserId, RedisService.Characterdata_int.heroSlot, "2");
                                            }
                                            break;
                                    }
                                }
                            }
                        }
                        break;
                }
            }

            return ReturnData;
        }


    }
}


