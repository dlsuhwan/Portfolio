using Google.Apis.Auth.OAuth2;
using LibraryRedisClass;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Server.Services;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using static LibraryRedisClass.RedisService;

namespace Server.Server.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ManageToolController : ControllerBase
    {
        private readonly RedisService _redisService;
        private readonly AuthService _authService;
        private readonly string _logDirectory;
        private static readonly Random _random = new Random();

        public ManageToolController(RedisService redisService, AuthService authService)
        {
            _redisService = redisService;
            _authService = authService;

            string logDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs");

            string logDir = Path.Combine(logDirectory, "ManageToolLog");

            _logDirectory = logDir;

            // 파일에 로그 추가
            if (!Directory.Exists(logDir)) Directory.CreateDirectory(logDir);
        }

        [HttpGet("session")]
        public IActionResult Session()
        {
            var token = Request.Cookies["session_token"];
            if (string.IsNullOrWhiteSpace(token))
            {
                return Unauthorized(new { ok = false, message = "no_session" });
            }

            return Ok(new { ok = true });
        }
        [HttpGet("menu")]
        public IActionResult Menu()
        {
            return Ok(new[]
            {
                new { key = "home", label = "홈" },
                new { key = "db", label = "DB정보" },
                new { key = "user", label = "유저 정보" },
                new { key = "mail", label = "우편" },
                new { key = "coupon", label = "쿠폰" },
                new { key = "ops", label = "운영" },
                new { key = "serverstate", label = "서버상황" }
            });
        }

        public class RequestList
        {
            public string Value1 { get; set; } = "";
            public string Value2 { get; set; } = "";
            public string Value3 { get; set; } = "";
            public string Value4 { get; set; } = "";
            public string Value5 { get; set; } = "";
            public string Value6 { get; set; } = "";
            public string Value7 { get; set; } = "";
            public string Value8 { get; set; } = "";
            public string Value9 { get; set; } = "";
            public string Value10 { get; set; } = "";
        }
        [NonAction]
        public float GetRandomFloat(float start, float end)
        {
            if (end < start)
                throw new ArgumentException("end는 start보다 커야 합니다.");

            double range = end - start;
            double value = start + (_random.NextDouble() * range);
            return (float)Math.Round(value, 1);
        }
      
        [HttpPost("searchuserid")]
        public async Task<IActionResult> SearchUserId([FromBody] RequestList request)
        {
            if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

            string userId = request.Value1;

            if (userId != "")
            {
                string nickname = await _redisService.GetUserStringData(userId, RedisService.Characterdata_string.nickname);
                if (nickname != "")
                    return Ok(new { ok = true, userId = userId, nickname = nickname });
                else
                    return Unauthorized(new { ok = false, message = "not found data" });
            }
            else
            {
                return Unauthorized(new { ok = false, message = "not found data" });
            }
        }
        [HttpPost("searchnickname")]
        public async Task<IActionResult> SearchNickname([FromBody] RequestList request)
        {
            if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

            string nickname = request.Value1;

            if (nickname != "")
            {
                string userId = await _redisService.GetUserIDByNick(nickname);
                if (userId != "")
                    return Ok(new { ok = true, userId = userId, nickname = nickname });
                else
                    return Unauthorized(new { ok = false, message = "not found data" });
            }
            else
            {
                return Unauthorized(new { ok = false, message = "not found data" });
            }
        }

        public class RedisKeyResponse
        {
            public string Key { get; set; } = "";
        }

        [HttpPost("keys")]
        public async Task<IActionResult> GetKeys([FromBody] RequestList request)
        {
            // 인증 체크 (session_token)
            if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

            // Redis에서 sector별 Key 조회
            RedisValue[] keys = null;
            if (request.Value1 == "user") keys = await _redisService.GetKeys_UserRedis();
            else if (request.Value1 == "total") keys = await _redisService.GetKeys_TotalRedis();
            else if (request.Value1 == "template") keys = await _redisService.GetKeys_DataRedis();

            if (keys != null)
            {
                var keyStrings = keys.Select(k => k).ToArray();

                // 결과 반환
                var result = keyStrings.Select(k => new RedisKeyResponse { Key = k });
                return Ok(new { ok = true, keys = result });
            }
            else
            {
                return Unauthorized(new { ok = false, message = "not found data" });
            }
        }

        public class RedisFieldResponse
        {
            public string sector { get; set; } = "";
            public string key { get; set; } = "";
            public string Field { get; set; } = "";
            public string Value { get; set; } = "";
        }

        [HttpPost("fields")]
        public async Task<IActionResult> GetFields([FromBody] RequestList request)
        {
            // 인증 체크 (session_token)
            if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

            string sector = request.Value1;
            string key = request.Value2;

            string keyType = "";
            if (sector == "user") keyType = await _redisService.GetRedisType(key, RedisService.redis_.user);
            else if (sector == "total") keyType = await _redisService.GetRedisType(key, RedisService.redis_.total);
            else if (sector == "template") keyType = await _redisService.GetRedisType(key, RedisService.redis_.data);

            List<RedisFieldResponse> result = new List<RedisFieldResponse>();

            switch (keyType)
            {
                case "key":
                    {
                        string value = "";
                        if (sector == "user") value = await _redisService.GetUserValue(key);
                        else if (sector == "total") value = await _redisService.GetTotalValue(key);
                        else if (sector == "template") value = await _redisService.GetDataValue(key);

                        result.Add(new RedisFieldResponse
                        {
                            sector = sector,
                            key = key,
                            Field = value,
                            Value = ""
                        });
                    }
                    break;
                case "H":
                    {
                        HashEntry[] hashEntry = null;
                        if (sector == "user") hashEntry = await _redisService.HGetAllUserValue(key);
                        else if (sector == "total") hashEntry = await _redisService.HGetAllTotalValue(key);
                        else if (sector == "template") hashEntry = await _redisService.HGetAllDataValue(key);

                        if (hashEntry != null)
                        {
                            string hashStr = "";
                            foreach (var hash in hashEntry)
                            {
                                string field = _redisService.RedisToString(hash.Name);
                                string data = _redisService.RedisToString(hash.Value);

                                result.Add(new RedisFieldResponse
                                {
                                    sector = sector,
                                    key = key,
                                    Field = field,
                                    Value = data
                                });
                            }
                        }
                    }
                    break;
                case "Z":
                    {

                    }
                    break;
            }

            if (result.Count > 0) return Ok(new { ok = true, fields = result });
            else return Unauthorized(new { ok = false, message = "not found data" });
        }

        public class UserintDataResponse
        {
            public string intkey { get; set; } = "";
            public string intdata { get; set; } = "";
        }
        public class UserstringDataResponse
        {
            public string stringkey { get; set; } = "";
            public string stringdata { get; set; } = "";
        }

        [HttpPost("UserInfoByUserId")]
        public async Task<IActionResult> GetUserInfoByUserId([FromBody] RequestList request)
        {
            // 인증 체크 (session_token)
            if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

            // Redis에서 sector별 Key 조회
            string userID = request.Value1;
            string nickname = "";

            HashEntry[] UserDataInt = await _redisService.HGetAllUserIntData(userID);
            HashEntry[] UserDatastring = await _redisService.HGetAllUserStringData(userID);

            Dictionary<int, string> intdata = new Dictionary<int, string>();
            Dictionary<int, string> stringdata = new Dictionary<int, string>();

            List<UserintDataResponse> intList = new List<UserintDataResponse>();
            List<UserstringDataResponse> stringList = new List<UserstringDataResponse>();

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

            for (int i = 0; i < (int)Characterdata_int.max; i++)
            {
                string value = intdata.ContainsKey(i) ? intdata[i] : "0";
                intList.Add(new UserintDataResponse { intkey = ((RedisService.Characterdata_int)i).ToString(), intdata = value });
            }

            for (int i = 0; i < (int)Characterdata_string.max; i++)
            {
                string value = stringdata.ContainsKey(i) ? stringdata[i] : "";
                stringList.Add(new UserstringDataResponse { stringkey = ((RedisService.Characterdata_string)i).ToString(), stringdata = value });

                if (i == (int)Characterdata_string.nickname) nickname = value;
            }

            if (intList.Count > 0 && stringList.Count > 0)
            {
                return Ok(new { ok = true, intdata = intList, stringdata = stringList, userId = userID, nickname = nickname });
            }
            else
            {
                return Unauthorized(new { ok = false, message = "not found data" });
            }
        }

        [HttpPost("UserInfoByNickname")]
        public async Task<IActionResult> GetUserInfoByNickname([FromBody] RequestList request)
        {
            // 인증 체크 (session_token)
            if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

            // Redis에서 sector별 Key 조회
            string nickname = request.Value1;
            string userID = await _redisService.GetUserIDByNick(nickname);

            HashEntry[] UserDataInt = await _redisService.HGetAllUserIntData(userID);
            HashEntry[] UserDatastring = await _redisService.HGetAllUserStringData(userID);

            Dictionary<int, string> intdata = new Dictionary<int, string>();
            Dictionary<int, string> stringdata = new Dictionary<int, string>();

            List<UserintDataResponse> intList = new List<UserintDataResponse>();
            List<UserstringDataResponse> stringList = new List<UserstringDataResponse>();

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

            for (int i = 0; i < (int)Characterdata_int.max; i++)
            {
                string value = intdata.ContainsKey(i) ? intdata[i] : "0";
                intList.Add(new UserintDataResponse { intkey = ((RedisService.Characterdata_int)i).ToString(), intdata = value });
            }

            for (int i = 0; i < (int)Characterdata_string.max; i++)
            {
                string value = stringdata.ContainsKey(i) ? stringdata[i] : "";
                stringList.Add(new UserstringDataResponse { stringkey = ((RedisService.Characterdata_string)i).ToString(), stringdata = value });

                if (i == (int)Characterdata_string.nickname) nickname = value;
            }


            if (intList.Count > 0 && stringList.Count > 0)
            {
                return Ok(new { ok = true, intdata = intList, stringdata = stringList, userId = userID, nickname = nickname });
            }
            else
            {
                return Unauthorized(new { ok = false, message = "not found data" });
            }
        }

        [HttpPost("chageintdata")]
        public async Task<IActionResult> ChangeIntData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string index = request.Value2;
                string data = request.Value3;

                string nickname = await _redisService.GetUserStringData(UserId, RedisService.Characterdata_string.nickname);

                await _redisService.SetUserIntData(UserId, (RedisService.Characterdata_int)int.Parse(index), data);

                HashEntry[] UserDataInt = await _redisService.HGetAllUserIntData(UserId);
                HashEntry[] UserDatastring = await _redisService.HGetAllUserStringData(UserId);

                Dictionary<int, string> intdata = new Dictionary<int, string>();

                List<UserintDataResponse> intList = new List<UserintDataResponse>();

                for (int i = 0; i < UserDataInt.Length; i++)
                {
                    string field = _redisService.RedisToString(UserDataInt[i].Name);
                    string value = _redisService.RedisToString(UserDataInt[i].Value);
                    intdata.Add(int.Parse(field), value);
                }

                for (int i = 0; i < (int)Characterdata_int.max; i++)
                {
                    string value = intdata.ContainsKey(i) ? intdata[i] : "0";
                    intList.Add(new UserintDataResponse { intkey = ((RedisService.Characterdata_int)i).ToString(), intdata = value });
                }

                if (intList.Count > 0)
                {
                    return Ok(new { ok = true, intdata = intList, userId = UserId, nickname = nickname });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("chagestringdata")]
        public async Task<IActionResult> ChangeStringData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string index = request.Value2;
                string data = request.Value3;

                string nickname = "";

                if (int.Parse(index) == (int)Characterdata_string.nickname)
                {
                    if (await _redisService.CheckNickname(data) == false)
                    {
                        await _redisService.SetNickname(data, UserId);
                    }
                    else
                    {
                        return Unauthorized(new { ok = false, message = "already has nick" });
                    }
                }

                await _redisService.SetUserStringData(UserId, (RedisService.Characterdata_string)int.Parse(index), data);

                HashEntry[] UserDataInt = await _redisService.HGetAllUserIntData(UserId);
                HashEntry[] UserDatastring = await _redisService.HGetAllUserStringData(UserId);

                Dictionary<int, string> stringdata = new Dictionary<int, string>();

                List<UserstringDataResponse> stringList = new List<UserstringDataResponse>();

                for (int i = 0; i < UserDatastring.Length; i++)
                {
                    string field = _redisService.RedisToString(UserDatastring[i].Name);
                    string value = _redisService.RedisToString(UserDatastring[i].Value);
                    stringdata.Add(int.Parse(field), value);
                }

                for (int i = 0; i < (int)Characterdata_string.max; i++)
                {
                    string value = stringdata.ContainsKey(i) ? stringdata[i] : "";
                    stringList.Add(new UserstringDataResponse { stringkey = ((RedisService.Characterdata_string)i).ToString(), stringdata = value });

                    if (i == (int)Characterdata_string.nickname) nickname = value;
                }

                if (stringList.Count > 0)
                {
                    return Ok(new { ok = true, stringdata = stringList, userId = UserId, nickname = nickname });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        public class QuestCountResponse
        {
            public string index { get; set; } = "";
            public string data { get; set; } = "";
        }
        public class QuestDataResponse
        {
            public string index { get; set; } = "";
            public string data { get; set; } = "";
        }


        [HttpPost("getquest")]
        public async Task<IActionResult> GetQuest([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string questType = request.Value2;

                HashEntry[] QuestCount = await _redisService.GetQuestCount(UserId, questType);
                HashEntry[] QuestData = await _redisService.GetQuestData(UserId, questType);

                Dictionary<string, string> countDic = new Dictionary<string, string>();
                Dictionary<string, string> dataDic = new Dictionary<string, string>();

                for (int i = 0; i < QuestCount.Length; i++)
                {
                    string questIndex = _redisService.RedisToString(QuestCount[i].Name);
                    string questcount = _redisService.RedisToString(QuestCount[i].Value);

                    countDic.Add(questIndex, questcount);
                }
                for (int i = 0; i < QuestData.Length; i++)
                {
                    string questIndex = _redisService.RedisToString(QuestData[i].Name);
                    string questData = _redisService.RedisToString(QuestData[i].Value);

                    dataDic.Add(questIndex, questData);
                }

                List<QuestCountResponse> questCountList = new List<QuestCountResponse>();
                List<QuestDataResponse> questDataList = new List<QuestDataResponse>();


                for (int i = 0; i < (int)QuestIndex_.max; i++)
                {
                    string countValue = countDic.ContainsKey(i.ToString()) ? countDic[i.ToString()] : "0";
                    questCountList.Add(new QuestCountResponse { index = ((RedisService.QuestIndex_)i).ToString(), data = countValue });


                    string dataValue = dataDic.ContainsKey(i.ToString()) ? dataDic[i.ToString()] : "0";
                    questDataList.Add(new QuestDataResponse { index = ((RedisService.QuestIndex_)i).ToString(), data = dataValue });
                }

                if (questCountList.Count > 0 && questDataList.Count > 0)
                {
                    return Ok(new { ok = true, questcount = questCountList, questdata = questDataList });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }


        [HttpPost("changequestcount")]
        public async Task<IActionResult> ChangeQuestCount([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string questType = request.Value2;
                string index = request.Value3;
                string data = request.Value4;

                await _redisService.SetQuestCount(UserId, questType, index, data);

                HashEntry[] QuestCount = await _redisService.GetQuestCount(UserId, questType);

                Dictionary<string, string> countDic = new Dictionary<string, string>();

                for (int i = 0; i < QuestCount.Length; i++)
                {
                    string questIndex = _redisService.RedisToString(QuestCount[i].Name);
                    string questcount = _redisService.RedisToString(QuestCount[i].Value);

                    countDic.Add(questIndex, questcount);
                }

                List<QuestCountResponse> questCountList = new List<QuestCountResponse>();


                for (int i = 0; i < (int)QuestIndex_.max; i++)
                {
                    string countValue = countDic.ContainsKey(i.ToString()) ? countDic[i.ToString()] : "0";
                    questCountList.Add(new QuestCountResponse { index = ((RedisService.QuestIndex_)i).ToString(), data = countValue });
                }

                if (questCountList.Count > 0)
                {
                    return Ok(new { ok = true, questcount = questCountList });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("changequestdata")]
        public async Task<IActionResult> ChangeQuestData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string questType = request.Value2;
                string index = request.Value3;
                string data = request.Value4;

                await _redisService.SetQuestData(UserId, questType, index, data);

                HashEntry[] QuestData = await _redisService.GetQuestData(UserId, questType);

                Dictionary<string, string> DataDic = new Dictionary<string, string>();

                for (int i = 0; i < QuestData.Length; i++)
                {
                    string questIndex = _redisService.RedisToString(QuestData[i].Name);
                    string questdata = _redisService.RedisToString(QuestData[i].Value);

                    DataDic.Add(questIndex, questdata);
                }

                List<QuestDataResponse> questDataList = new List<QuestDataResponse>();

                for (int i = 0; i < (int)QuestIndex_.max; i++)
                {
                    string dataValue = DataDic.ContainsKey(i.ToString()) ? DataDic[i.ToString()] : "0";
                    questDataList.Add(new QuestDataResponse { index = ((RedisService.QuestIndex_)i).ToString(), data = dataValue });
                }

                if (questDataList.Count > 0)
                {
                    return Ok(new { ok = true, questdata = questDataList });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        public class ShopPurchaseResponse
        {
            public string shopindex { get; set; } = "";
            public string buycount { get; set; } = "";
        }
        public class PacakgePurchaseResponse
        {
            public string packageindex { get; set; } = "";
            public string buycount { get; set; } = "";
        }
        [HttpPost("getinapp")]
        public async Task<IActionResult> GetInapp([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;

                HashEntry[] ShoPurchasepEntry = await _redisService.GetUserShopPurchase(UserId);
                HashEntry[] packagePurchaseEntry = await _redisService.GetUserPackagePurchase(UserId);

                List<ShopPurchaseResponse> ShopPurchaseList = new List<ShopPurchaseResponse>();
                List<PacakgePurchaseResponse> PackagePurchaseList = new List<PacakgePurchaseResponse>();

                for (int i = 0; i < ShoPurchasepEntry.Length; i++)
                {
                    string data = _redisService.RedisToString(ShoPurchasepEntry[i].Value);
                    if (data != "")
                    {
                        string[] datas = data.Split('#');

                        string shopIndex = datas[(int)User_shopPurchase.index];
                        string buycount = datas[(int)User_shopPurchase.buyCount];

                        ShopPurchaseList.Add(new ShopPurchaseResponse { shopindex = shopIndex, buycount = buycount });
                    }
                }

                for (int i = 0; i < packagePurchaseEntry.Length; i++)
                {
                    string data = _redisService.RedisToString(packagePurchaseEntry[i].Value);

                    if (data != "")
                    {
                        string[] datas = data.Split('#');
                        string packageIndex = datas[(int)User_packagePurchase.index];
                        string buycount = datas[(int)User_packagePurchase.buyCount];

                        PackagePurchaseList.Add(new PacakgePurchaseResponse { packageindex = packageIndex, buycount = buycount });
                    }
                }


                if (ShopPurchaseList.Count > 0 && PackagePurchaseList.Count > 0)
                {
                    return Ok(new { ok = true, shopdata = ShopPurchaseList, packagedata = PackagePurchaseList });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }


        [HttpPost("changeshoppurchase")]
        public async Task<IActionResult> ChangeShopPurchase([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string index = request.Value2;
                string data = request.Value3;

                string shopdata = await _redisService.GetUserShopPurchase(UserId, index);
                if (shopdata == "") return Unauthorized(new { ok = false, message = "not found shop data" });

                string[] shopdatas = shopdata.Split('#');
                shopdatas[(int)(int)User_shopPurchase.index] = index;
                shopdatas[(int)(int)User_shopPurchase.buyCount] = data;
                shopdatas[(int)(int)User_shopPurchase.nextTimingday] = _redisService.GetDateTimeNow();
                await _redisService.SetUserShopPurchase(UserId, index, string.Join("#", shopdatas));

                HashEntry[] ShoPurchasepEntry = await _redisService.GetUserShopPurchase(UserId);

                List<ShopPurchaseResponse> ShopPurchaseList = new List<ShopPurchaseResponse>();

                for (int i = 0; i < ShoPurchasepEntry.Length; i++)
                {
                    string purchasedata = _redisService.RedisToString(ShoPurchasepEntry[i].Value);
                    if (purchasedata != "")
                    {
                        string[] purchasedatas = purchasedata.Split('#');

                        string shopIndex = purchasedatas[(int)User_shopPurchase.index];
                        string buycount = purchasedatas[(int)User_shopPurchase.buyCount];

                        ShopPurchaseList.Add(new ShopPurchaseResponse { shopindex = shopIndex, buycount = buycount });
                    }
                }

                if (ShopPurchaseList.Count > 0)
                {
                    return Ok(new { ok = true, shopdata = ShopPurchaseList });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("changepackagepurchase")]
        public async Task<IActionResult> ChangePackagePurchase([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string index = request.Value2;
                string data = request.Value3;

                string packagedata = await _redisService.GetUserPackagePurchase(UserId, index);
                if (packagedata == "") return Unauthorized(new { ok = false, message = "not found package data" });

                string[] packagedatas = packagedata.Split('#');
                packagedatas[(int)(int)User_packagePurchase.index] = index;
                packagedatas[(int)(int)User_packagePurchase.buyCount] = data;
                await _redisService.SetUserPackagePurchase(UserId, index, string.Join("#", packagedatas));

                HashEntry[] PackageurchasepEntry = await _redisService.GetUserPackagePurchase(UserId);

                List<PacakgePurchaseResponse> PackagePurchaseList = new List<PacakgePurchaseResponse>();

                for (int i = 0; i < PackageurchasepEntry.Length; i++)
                {
                    string purchasedata = _redisService.RedisToString(PackageurchasepEntry[i].Value);
                    if (purchasedata != "")
                    {
                        string[] purchasedatas = purchasedata.Split('#');

                        string shopIndex = purchasedatas[(int)User_packagePurchase.index];
                        string buycount = purchasedatas[(int)User_packagePurchase.buyCount];

                        PackagePurchaseList.Add(new PacakgePurchaseResponse { packageindex = shopIndex, buycount = buycount });
                    }
                }

                if (PackagePurchaseList.Count > 0)
                {
                    return Ok(new { ok = true, packagedata = PackagePurchaseList });
                }
                else
                {
                    return Unauthorized(new { ok = false, message = "not found data" });
                }
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("getpostlist")]
        public async Task<IActionResult> GetPostList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;

                HashEntry[] hashEntry = await _redisService.GetPostAll_hash(UserId);

                List<string> result = new List<string>();

                foreach (var entry in hashEntry)
                {
                    string postdata = _redisService.RedisToString(entry.Value);

                    if (postdata != "") result.Add(postdata);
                }

                return Ok(new { ok = true, postlist = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("delpostlist")]
        public async Task<IActionResult> DelPostList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string UserId = request.Value1;
                string postIndex = request.Value2;

                await _redisService.DelPost(UserId, postIndex);

                HashEntry[] hashEntry = await _redisService.GetPostAll_hash(UserId);

                List<string> result = new List<string>();

                foreach (var entry in hashEntry)
                {
                    string postdata = _redisService.RedisToString(entry.Value);

                    if (postdata != "") result.Add(postdata);
                }

                return Ok(new { ok = true, postlist = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("processpost")]
        public async Task<IActionResult> ProcessPost([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string userId = request.Value1;
                string title = request.Value2;
                string reward = request.Value3;
                string expireday = request.Value4 == "" ? "0" : request.Value5;
                string postType = request.Value5 == "" ? "0" : request.Value6;

                if (userId == "" || title == "" || reward == "" || expireday == "" || postType == "")
                    return Unauthorized(new { ok = false, message = "has null data" });

                string[] rewards = reward.Split('*');
                for (int i = 0; i < rewards.Length; i++)
                {
                    if (rewards[i] == "") continue;

                    string[] rewards_ = rewards[i].Split('^');
                    string rewardType = rewards_[0];
                    string rewardValue = rewards_[1];

                    if (rewardType == "" || rewardValue == "") continue;

                    await _redisService.ProcessPost(userId, rewardType, rewardValue, title, expireday, postType);
                }

                HashEntry[] hashEntry = await _redisService.GetPostAll_hash(userId);

                List<string> result = new List<string>();

                foreach (var entry in hashEntry)
                {
                    string postdata = _redisService.RedisToString(entry.Value);

                    if (postdata != "") result.Add(postdata);
                }

                return Ok(new { ok = true, postlist = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("getpostschedule")]
        public async Task<IActionResult> GetPostScheduleList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                HashEntry[] hashEntry = await _redisService.GetPostScheduleAll_hash();

                List<string> result = new List<string>();

                foreach (var entry in hashEntry)
                {
                    string postdata = _redisService.RedisToString(entry.Value);

                    if (postdata != "") result.Add(postdata);
                }

                return Ok(new { ok = true, postschedulelist = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("setpostschedule")]
        public async Task<IActionResult> SetPostScheduleList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string title = request.Value1;
                string reward = request.Value2;
                string startday = request.Value3;
                string endday = request.Value4;

                if (title == "" || reward == "" || startday == "" || endday == "")
                    return Unauthorized(new { ok = false, message = "has null data" });

                await _redisService.SetPostSchedule(reward, title, startday, endday);

                HashEntry[] hashEntry = await _redisService.GetPostScheduleAll_hash();

                List<string> result = new List<string>();

                foreach (var entry in hashEntry)
                {
                    string postdata = _redisService.RedisToString(entry.Value);

                    if (postdata != "") result.Add(postdata);
                }

                return Ok(new { ok = true, postschedulelist = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("delpostschedule")]
        public async Task<IActionResult> DelPostScheduleList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string index = request.Value1;

                if (index == "")
                    return Unauthorized(new { ok = false, message = "has null data" });

                await _redisService.DelPostSchedule(index);

                HashEntry[] hashEntry = await _redisService.GetPostScheduleAll_hash();

                List<string> result = new List<string>();

                foreach (var entry in hashEntry)
                {
                    string postdata = _redisService.RedisToString(entry.Value);

                    if (postdata != "") result.Add(postdata);
                }

                return Ok(new { ok = true, postschedulelist = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("getcoupon")]
        public async Task<IActionResult> GetCoupon([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                var names = await _redisService.GetCouponNames();

                List<string> list = new List<string>();
                foreach (var name in names)
                {
                    list.Add(_redisService.RedisToString(name));
                }


                string result = string.Join("|", list);

                return Ok(new { ok = true, value = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("delcoupon")]
        public async Task<IActionResult> DelCoupon([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string couponName = request.Value1;

                string couponDetail = await _redisService.GetCoupon(couponName);
                
                if (couponDetail == "") return Unauthorized(new { ok = false, message = $"없는 쿠폰 내역" });

                string couponUse = await _redisService.GetCouponUse(couponName);

                WriteLog($"delCouponList\n{couponDetail}\n{couponUse}");
                
                await _redisService.DelCoupon(couponName);
                await _redisService.DelCouponUse(couponName);

                var names = await _redisService.GetCouponNames();

                List<string> list = new List<string>();
                foreach (var name in names)
                {
                    list.Add(_redisService.RedisToString(name));
                }

                string result = string.Join("|", list);

                return Ok(new { ok = true, value = result });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("getcoupondetail")]
        public async Task<IActionResult> GetCouponDetail([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string couponName = request.Value1;

                var couponDetail = await _redisService.GetCoupon(couponName);

                return Ok(new { ok = true, value = couponDetail });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }


        [HttpPost("addcoupon")]
        public async Task<IActionResult> AddCoupon([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string couponName = request.Value1;
                string couponReward = request.Value2;
                string couponGroup = request.Value3;
                string couponType = request.Value4;
                string couponUseMaxCount = request.Value5;
                string couponStartTime = request.Value6;
                string couponEndTime = request.Value7;

                string[] coupondatas = new string[(int)Coupon_.max];
                coupondatas[(int)Coupon_.name] = couponName;
                coupondatas[(int)Coupon_.reward] = couponReward;
                coupondatas[(int)Coupon_.group] = couponGroup;
                coupondatas[(int)Coupon_.type] = couponType;
                coupondatas[(int)Coupon_.useMaxCount] = couponUseMaxCount;
                coupondatas[(int)Coupon_.startTime] = couponStartTime;
                coupondatas[(int)Coupon_.endTime] = couponEndTime;

                if (await _redisService.GetCoupon(couponName) != "") return Unauthorized(new { ok = false, message = $"already has coupon,,, {couponName}" });

                await _redisService.SetCoupon(couponName, string.Join("#", coupondatas));

                return Ok(new { ok = true });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("addrandomcoupon")]
        public async Task<IActionResult> AddRnadomCoupon([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string couponCount = request.Value1;
                string couponType = request.Value2;
                string couponGroup = request.Value3;
                string couponReward = request.Value4;
                string couponStartTime = request.Value5;
                string couponEndTime = request.Value6;

                int couponCount_ = int.Parse(couponCount);
                List<string> newCouponList = new List<string>();

                for (int i = 0; i < couponCount_; i++)
                {
                    string newCouponName = GenerateSecureRandomString(6);

                    // 이미 있는 쿠폰넘버면 다시 시도 10번 정도?
                    if (await _redisService.CheckCoupon(newCouponName) == true)
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            newCouponName = GenerateSecureRandomString(6);
                         
                            if (await _redisService.GetCoupon(newCouponName) == "") break;
                        }

                    }

                    if (await _redisService.CheckCoupon(newCouponName) == false)
                    {
                        string[] coupondatas = new string[(int)Coupon_.max];
                        coupondatas[(int)Coupon_.name] = newCouponName;
                        coupondatas[(int)Coupon_.reward] = couponReward;
                        coupondatas[(int)Coupon_.group] = couponGroup;
                        coupondatas[(int)Coupon_.type] = couponType;
                        coupondatas[(int)Coupon_.useMaxCount] = "1";
                        coupondatas[(int)Coupon_.startTime] = couponStartTime;
                        coupondatas[(int)Coupon_.endTime] = couponEndTime;

                        await _redisService.SetCoupon(newCouponName, string.Join("#", coupondatas));

                        newCouponList.Add(newCouponName);
                    }
                }

                //return Ok(new { ok = true, randomcouponlist = newCouponList });

                // 생성된 쿠폰들을 텍스트로 변환
                var json = System.Text.Json.JsonSerializer.Serialize(new
                {
                    ok = true,
                    randomcouponlist = newCouponList
                });

                byte[] fileBytes = System.Text.Encoding.UTF8.GetBytes(json);
                return File(fileBytes, "application/json", "coupons.json");
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        private string GenerateSecureRandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            StringBuilder result = new StringBuilder(length);

            using (var rng = RandomNumberGenerator.Create())
            {
                byte[] buffer = new byte[1];

                for (int i = 0; i < length; i++)
                {
                    rng.GetBytes(buffer);
                    int index = buffer[0] % chars.Length; // chars 배열 길이로 나머지
                    result.Append(chars[index]);
                }
            }

            return result.ToString();
        }

        [HttpPost("getInspection")]
        public async Task<IActionResult> GetInspection([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string curInspection = await _redisService.GetInspection();
                string curInspectionNoti = await _redisService.GetInspectionNoti();

                bool bState = false;
                if (curInspection == "0") bState = false;
                else bState = true;

                return Ok(new { ok = true, state = bState, notice = curInspectionNoti });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setInspection")]
        public async Task<IActionResult> SetInspection([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string inspectiondata = request.Value1;
                string inspectionnotidata = request.Value2;

                if (inspectiondata == "0") inspectionnotidata = "0";

                await _redisService.SetInspection(inspectiondata);
                await _redisService.SetInspectionNoti(inspectionnotidata);

                string curInspection = await _redisService.GetInspection();
                string curInspectionNoti = await _redisService.GetInspectionNoti();

                bool bState = false;
                if (curInspection == "0") bState = false;
                else bState = true;

                return Ok(new { ok = true, state = bState, notice = curInspectionNoti });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("getbattleInspection")]
        public async Task<IActionResult> GetBattleInspection([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string curInspection = await _redisService.GetBattleInspection();
                string curInspectionNoti = await _redisService.GetBattleInspectionNoti();

                bool bState = false;
                if (curInspection == "0") bState = false;
                else bState = true;

                return Ok(new { ok = true, state = bState, notice = curInspectionNoti });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setbattleinspection")]
        public async Task<IActionResult> SetBattleInspection([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string inspectiondata = request.Value1;
                string inspectionnotidata = request.Value2;

                if (inspectiondata == "0") inspectionnotidata = "0";

                await _redisService.SetBattleInspection(inspectiondata);
                await _redisService.SetBattleInspectionNoti(inspectionnotidata);

                string curInspection = await _redisService.GetBattleInspection();
                string curInspectionNoti = await _redisService.GetBattleInspectionNoti();

                bool bState = false;
                if (curInspection == "0") bState = false;
                else bState = true;

                return Ok(new { ok = true, state = bState, notice = curInspectionNoti });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("getnoti")]
        public async Task<IActionResult> GetNoti([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string noti = await _redisService.GetNotice();

                List<string> notiList = new List<string>();
                if (noti != "") notiList = noti.Split('#').ToList();

                return Ok(new { ok = true, notilist = notiList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setnoti")]
        public async Task<IActionResult> SetNoti([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });


                string beforedata = await _redisService.GetNotice();

                if (beforedata == "") beforedata = request.Value1;
                else beforedata = request.Value1 + "#" + beforedata;

                await _redisService.SetNotice(beforedata);
                await _redisService.SetNoticeVersion();

                string noti = await _redisService.GetNotice();

                List<string> notiList = new List<string>();
                if (noti != "") notiList = noti.Split('#').ToList();

                return Ok(new { ok = true, notilist = notiList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delnoti")]
        public async Task<IActionResult> DelNoti([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string beforedata = await _redisService.GetNotice();
                List<string> beforedatas = beforedata.Split('#').ToList();

                if (beforedatas.Contains(request.Value1)) beforedatas.Remove(request.Value1);

                string afterdata = string.Join("#", beforedatas);

                if (afterdata != "") await _redisService.SetNotice(afterdata);
                else await _redisService.DelNotice();

                string noti = await _redisService.GetNotice();

                List<string> notiList = new List<string>();
                if (noti != "") notiList = noti.Split('#').ToList();

                return Ok(new { ok = true, notilist = notiList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        public class MessageToastResponse
        {
            public string index { get; set; } = "";
            public string starttime { get; set; } = "";
            public string endtime { get; set; } = "";
            public string message { get; set; } = "";
        }

        [HttpPost("getmangetoast")]
        public async Task<IActionResult> GetMangeToast([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string date = request.Value1;
                DateTime dt = DateTime.Parse(date).ToLocalTime();
                date = dt.ToString("yyyy-MM-dd");

                HashEntry[] managetoasHash = await _redisService.GetManageToastAll(date);

                List<MessageToastResponse> managetoastlist = new List<MessageToastResponse>();

                for (int i = 0; i < managetoasHash.Length; i++)
                {
                    string toastindex = _redisService.RedisToString(managetoasHash[i].Name);
                    string[] toastdata = _redisService.RedisToString(managetoasHash[i].Value).Split('#');
                    managetoastlist.Add(new MessageToastResponse { index = toastindex, starttime = toastdata[0], endtime = toastdata[1], message = toastdata[2] });
                }

                return Ok(new { ok = true, toastdata = managetoastlist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setmangetoast")]
        public async Task<IActionResult> SetMangeToast([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string date = request.Value1;
                string index = request.Value2;
                string Starttime = request.Value3;
                string EndTime = request.Value4;
                string Data = request.Value5;

                DateTime dt = DateTime.Parse(date).ToLocalTime();
                date = dt.ToString("yyyy-MM-dd");

                await _redisService.SetManageToast(date, index, Starttime + "#" + EndTime + "#" + Data);


                HashEntry[] managetoasHash = await _redisService.GetManageToastAll(date);

                List<MessageToastResponse> managetoastlist = new List<MessageToastResponse>();

                for (int i = 0; i < managetoasHash.Length; i++)
                {
                    string toastindex = _redisService.RedisToString(managetoasHash[i].Name);
                    string[] toastdata = _redisService.RedisToString(managetoasHash[i].Value).Split('#');
                    managetoastlist.Add(new MessageToastResponse { index = toastindex, starttime = toastdata[0], endtime = toastdata[1], message = toastdata[2] });
                }

                return Ok(new { ok = true, toastdata = managetoastlist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delmanagetoast")]
        public async Task<IActionResult> DelManageToast([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string date = request.Value1;
                string index = request.Value2;

                DateTime dt = DateTime.Parse(date).ToLocalTime();
                date = dt.ToString("yyyy-MM-dd");

                await _redisService.DelManageToast(date, index);

                HashEntry[] managetoasHash = await _redisService.GetManageToastAll(date);

                List<MessageToastResponse> managetoastlist = new List<MessageToastResponse>();

                for (int i = 0; i < managetoasHash.Length; i++)
                {
                    string toastindex = _redisService.RedisToString(managetoasHash[i].Name);
                    string[] toastdata = _redisService.RedisToString(managetoasHash[i].Value).Split('#');
                    managetoastlist.Add(new MessageToastResponse { index = toastindex, starttime = toastdata[0], endtime = toastdata[1], message = toastdata[2] });
                }

                return Ok(new { ok = true, toastdata = managetoastlist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        //benlist
        [HttpPost("getbanlist")]
        public async Task<IActionResult> GetBanList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                HashEntry[] banlistHash = await _redisService.GetBanListAll();

                List<string> banlist = new List<string>();
                for (int i = 0; i < banlistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(banlistHash[i].Name);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    banlist.Add(nickname);
                }

                return Ok(new { ok = true, banlist = banlist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setbanlist")]
        public async Task<IActionResult> SetBanList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string userid = request.Value1;

                if (userid == "") return Unauthorized(new { ok = false, message = "no userid" });

                await _redisService.SetBanList(userid);

                HashEntry[] banlistHash = await _redisService.GetBanListAll();

                List<string> banlist = new List<string>();
                for (int i = 0; i < banlistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(banlistHash[i].Name);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    banlist.Add(nickname);
                }

                return Ok(new { ok = true, banlist = banlist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delbanlist")]
        public async Task<IActionResult> DelBanList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                if (request.Value1 == "") return Unauthorized(new { ok = false, message = "no nickname" });

                string userid = await _redisService.GetUserIDByNick(request.Value1);

                if (userid == "") return Unauthorized(new { ok = false, message = "not find nickname" });

                await _redisService.DelBanList(userid);

                HashEntry[] banlistHash = await _redisService.GetBanListAll();

                List<string> banlist = new List<string>();
                for (int i = 0; i < banlistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(banlistHash[i].Name);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    banlist.Add(nickname);
                }

                return Ok(new { ok = true, banlist = banlist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        //whitelist
        [HttpPost("getwhitelist")]
        public async Task<IActionResult> GetWhiteList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                HashEntry[] whitelistHash = await _redisService.GetWhiteListAll();

                List<string> whitelist = new List<string>();
                for (int i = 0; i < whitelistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(whitelistHash[i].Name);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    whitelist.Add(nickname);
                }

                return Ok(new { ok = true, whitelist = whitelist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setwhitelist")]
        public async Task<IActionResult> SetWhiteList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string userid = request.Value1;

                if (userid == "") return Unauthorized(new { ok = false, message = "no userid" });

                await _redisService.SetWhiteList(userid);

                HashEntry[] whitelistHash = await _redisService.GetWhiteListAll();

                List<string> whitelist = new List<string>();
                for (int i = 0; i < whitelistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(whitelistHash[i].Name);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    whitelist.Add(nickname);
                }

                return Ok(new { ok = true, whitelist = whitelist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delwhitelist")]
        public async Task<IActionResult> DelWhiteList([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                if (request.Value1 == "") return Unauthorized(new { ok = false, message = "no nickname" });

                string userid = await _redisService.GetUserIDByNick(request.Value1);

                if (userid == "") return Unauthorized(new { ok = false, message = "not find nickname" });

                await _redisService.DelWhiteList(userid);

                HashEntry[] whitelistHash = await _redisService.GetWhiteListAll();

                List<string> whitelist = new List<string>();
                for (int i = 0; i < whitelistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(whitelistHash[i].Name);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    whitelist.Add(nickname);
                }

                return Ok(new { ok = true, whitelist = whitelist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        //loginlink
        [HttpPost("getloginlink")]
        public async Task<IActionResult> GetLoginLink([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                HashEntry[] loginlinkistHash = await _redisService.GetAllloginlinkid();

                List<string> loginlinklist = new List<string>();
                for (int i = 0; i < loginlinkistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(loginlinkistHash[i].Value);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    loginlinklist.Add(nickname);
                }

                return Ok(new { ok = true, loginlinklist = loginlinklist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("setloginlink")]
        public async Task<IActionResult> SetLoginLink([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string firebaseId = request.Value1;
                string guestId = request.Value2;

                if (firebaseId == "" || guestId == "") return Unauthorized(new { ok = false, message = "no firebaseid, guestid" });

                await _redisService.setloginlinkid(firebaseId, guestId);

                HashEntry[] loginlinkistHash = await _redisService.GetAllloginlinkid();

                List<string> loginlinklist = new List<string>();
                for (int i = 0; i < loginlinkistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(loginlinkistHash[i].Value);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    loginlinklist.Add(nickname);
                }

                return Ok(new { ok = true, loginlinklist = loginlinklist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delloginlink")]
        public async Task<IActionResult> DelLoginLink([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                if (request.Value1 == "") return Unauthorized(new { ok = false, message = "no nickname" });

                string userid = await _redisService.GetUserIDByNick(request.Value1);

                if (userid == "") return Unauthorized(new { ok = false, message = "not find nickname" });

                HashEntry[] loginlinkistHash = await _redisService.GetAllloginlinkid();

                for (int i = 0; i < loginlinkistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(loginlinkistHash[i].Value);

                    if(userid == curuserid)
                    {
                        string firebaseid = _redisService.RedisToString(loginlinkistHash[i].Name);

                        await _redisService.delloginlinkid(firebaseid);
                    }
                }

                loginlinkistHash = await _redisService.GetAllloginlinkid();

                List<string> loginlinklist = new List<string>();
                for (int i = 0; i < loginlinkistHash.Length; i++)
                {
                    string curuserid = _redisService.RedisToString(loginlinkistHash[i].Value);

                    string nickname = await _redisService.GetUserStringData(curuserid, RedisService.Characterdata_string.nickname);

                    loginlinklist.Add(nickname);
                }

                return Ok(new { ok = true, loginlinklist = loginlinklist });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }


        [HttpPost("dauccuday")]
        public async Task<IActionResult> GetDauCcuDay([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string dayKey = request.Value1;

                DateTime dtdate = DateTime.Parse(dayKey);

                // 1분 단위 CCU/NRU, 하루 총 DAU 가져오기
                var ccuHash = await _redisService.GetAllUserCCU(dayKey);
                var nruHash = await _redisService.GetAllUserNRU(dayKey);
                int totalDau = await _redisService.GetUserDAU(dayKey);
                int totalNru = await _redisService.GetUserTotalNRU(dayKey);

                var data = Enumerable.Range(0, 24 * 60).Select(i =>
                {
                    string time = dtdate.AddMinutes(i).ToString("HH:mm");

                    // CCU 안전 처리
                    var ccuEntry = ccuHash.FirstOrDefault(x => _redisService.RedisToString(x.Name) == time);
                    int ccu = ccuEntry.Equals(default(HashEntry)) || ccuEntry.Value.IsNull
                        ? 0
                        : int.Parse(_redisService.RedisToString(ccuEntry.Value));

                    // NRU 안전 처리
                    var nruEntry = nruHash.FirstOrDefault(x => _redisService.RedisToString(x.Name) == time);
                    int nru = nruEntry.Equals(default(HashEntry)) || nruEntry.Value.IsNull
                        ? 0
                        : int.Parse(_redisService.RedisToString(nruEntry.Value));

                    // DAU는 전체 하루 단위라, 분 단위 표시에는 그대로 넣거나 누적 평균/0 처리 가능
                    return new
                    {
                        minute = time,
                        ccu = ccu,
                        nru = nru,
                    };
                }).ToList();

                return Ok(new { ok = true, data = data, totaldau = totalDau, totalnru = totalNru });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("getuserpayment")]
        public async Task<IActionResult> GetUserPayment([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string dayKey = request.Value1;

                long totalpaymentprice = await _redisService.GetUserUserTotalPaymenet(dayKey);
                long totalpaymentprice_google = await _redisService.GetUserUserTotalPaymenet_Store(dayKey, "playstore");
                long totalpaymentprice_onestore = await _redisService.GetUserUserTotalPaymenet_Store(dayKey, "onestore");
                long totalpaymentprice_apple = await _redisService.GetUserUserTotalPaymenet_Store(dayKey, "appstore");
                HashEntry[] paymentData = await _redisService.HGetAllUserPaymenetData(dayKey);

                List<string> paymentList = new List<string>();
                for (int i = 0; i < paymentData.Length; i++)
                {
                    string data = _redisService.RedisToString(paymentData[i].Value);
                    string[] datas = data.Split('#');

                    datas[1] = await _redisService.GetUserStringData(datas[1], Characterdata_string.nickname);

                    if (data != "") paymentList.Add(string.Join("#", datas));
                }

                return Ok(new { ok = true, totalprice = totalpaymentprice, googleprice = totalpaymentprice_google,
                    onestoreprice = totalpaymentprice_onestore, appleprice = totalpaymentprice_apple, paymentdata = paymentList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        public class RankingResponse
        {
            public string rank { get; set; } = "";
            public string nickname { get; set; } = "";
            public string score { get; set; } = "";
        }

        [HttpPost("getunlimitranking")]
        public async Task<IActionResult> GetUnlimitRanking([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string rankStart = request.Value1;
                string rankEnd = request.Value2;
                string stageIndex = request.Value3;

                int.TryParse(rankStart, out int startIndex);
                int.TryParse(rankEnd, out int endIndex);

                string key = $"{RedisService.RankUnlimitMode}_{stageIndex}_{_redisService.GetDateTimeTotalWeek()}";

                var rankingData = await _redisService.GetRankingRange(key, startIndex - 1, endIndex - 1);

                List<RankingResponse> rankingList = new List<RankingResponse>();

                foreach (var data in rankingData)
                {
                    string rankUserId = _redisService.RedisToString(data.Element);
                    double point = data.Score;
                    string score = ((long)Math.Floor(point)).ToString();

                    string nickname = await _redisService.GetUserStringData(rankUserId, RedisService.Characterdata_string.nickname);
                    long rank = await _redisService.GetRanking(key, rankUserId);

                    rankingList.Add(new RankingResponse { rank = rank.ToString(), nickname = nickname, score = score });
                }

                return Ok(new { ok = true, unlimitrankdata = rankingList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("getbattleranking")]
        public async Task<IActionResult> GetBattleRanking([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string rankStart = request.Value1;
                string rankEnd = request.Value2;

                int.TryParse(rankStart, out int startIndex);
                int.TryParse(rankEnd, out int endIndex);

                string key = $"{RedisService.RankBattleMode}_{_redisService.GetDateTimeTotalWeek()}";

                var rankingData = await _redisService.GetRankingRange(key, startIndex - 1, endIndex - 1);

                List<RankingResponse> rankingList = new List<RankingResponse>();

                foreach (var data in rankingData)
                {
                    string rankUserId = _redisService.RedisToString(data.Element);
                    double point = data.Score;
                    string score = ((long)Math.Floor(point)).ToString();

                    string nickname = await _redisService.GetUserStringData(rankUserId, RedisService.Characterdata_string.nickname);
                    long rank = await _redisService.GetRanking(key, rankUserId) + 1;

                    rankingList.Add(new RankingResponse { rank = rank.ToString(), nickname = nickname, score = score });
                }

                return Ok(new { ok = true, battlerankdata = rankingList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delunlimitranking")]
        public async Task<IActionResult> DelUnlimitRanking([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string rankStart = request.Value1;
                string rankEnd = request.Value2;
                string delNickname = request.Value3;
                string stageIndex = request.Value4;
                string userid = await _redisService.GetUserIDByNick(delNickname);

                int.TryParse(rankStart, out int startIndex);
                int.TryParse(rankEnd, out int endIndex);

                string key = $"{RedisService.RankUnlimitMode}_{stageIndex}_{_redisService.GetDateTimeTotalWeek()}";

                await _redisService.DelRanking(key, userid);
                await _redisService.SetUserIntData(userid, RedisService.Characterdata_int.UnlimitFlyingHighScore, "0");

                var rankingData = await _redisService.GetRankingRange(key, startIndex - 1, endIndex - 1);

                List<RankingResponse> rankingList = new List<RankingResponse>();

                foreach (var data in rankingData)
                {
                    string rankUserId = _redisService.RedisToString(data.Element);
                    double point = data.Score;
                    string score = ((long)Math.Floor(point)).ToString();

                    string nickname = await _redisService.GetUserStringData(rankUserId, RedisService.Characterdata_string.nickname);
                    long rank = await _redisService.GetRanking(key, rankUserId) + 1;

                    rankingList.Add(new RankingResponse { rank = rank.ToString(), nickname = nickname, score = score });
                }

                return Ok(new { ok = true, unlimitrankdata = rankingList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("delbattleranking")]
        public async Task<IActionResult> DelBattleRanking([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string rankStart = request.Value1;
                string rankEnd = request.Value2;
                string delNickname = request.Value3;
                string userid = await _redisService.GetUserIDByNick(delNickname);

                int.TryParse(rankStart, out int startIndex);
                int.TryParse(rankEnd, out int endIndex);

                string key = $"{RedisService.RankBattleMode}_{_redisService.GetDateTimeTotalWeek()}";

                await _redisService.DelRanking(key, userid);

                var rankingData = await _redisService.GetRankingRange(key, startIndex - 1, endIndex - 1);

                List<RankingResponse> rankingList = new List<RankingResponse>();

                foreach (var data in rankingData)
                {
                    string rankUserId = _redisService.RedisToString(data.Element);
                    double point = data.Score;
                    string score = ((long)Math.Floor(point)).ToString();

                    string nickname = await _redisService.GetUserStringData(rankUserId, RedisService.Characterdata_string.nickname);
                    long rank = await _redisService.GetRanking(key, rankUserId);

                    rankingList.Add(new RankingResponse { rank = rank.ToString(), nickname = nickname, score = score });
                }

                return Ok(new { ok = true, battlerankdata = rankingList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("firebasepush")]
        public async Task<IActionResult> FirebasePush([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string fmcTitle = request.Value1;
                string fmcData = request.Value2;

                GoogleCredential credential;
                string baseDir = AppContext.BaseDirectory;
                string serviceAccountPath = System.IO.Path.Combine(baseDir, "firebase_service_account.json");
                string FirebaseProjectId = "noom-3c22b";

                if (!System.IO.File.Exists(serviceAccountPath))
                {
                    return Unauthorized(new { ok = false, message = $"서비스 계정 JSON 파일을 찾을 수 없습니다" });
                }

                using (var stream = System.IO.File.OpenRead(serviceAccountPath))
                {
                    credential = GoogleCredential.FromStream(stream)
                        .CreateScoped("https://www.googleapis.com/auth/firebase.messaging");
                }

                var token = await credential.UnderlyingCredential.GetAccessTokenForRequestAsync();

                var fcmUrl = $"https://fcm.googleapis.com/v1/projects/{FirebaseProjectId}/messages:send";

                var message = new
                {
                    message = new
                    {
                        topic = "all_users",
                        notification = new
                        {
                            title = fmcTitle,
                            body = fmcData
                        },
                    }
                };

                var json = JsonConvert.SerializeObject(message);

                using var client = new HttpClient();
                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = await client.PostAsync(fcmUrl, content);
                var responseBody = await response.Content.ReadAsStringAsync();

                if (response.IsSuccessStatusCode)
                    return Ok(new { ok = true });
                else
                    return Unauthorized(new { ok = false, message = $"FCM 발송 실패: {responseBody}" });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        public class RetensionResponse
        {
            public string date { get; set; } = "";
            public long count { get; set; } 
            public float rate { get; set; }
        }
        [HttpPost("userretentiondata")]
        public async Task<IActionResult> UserRetentionData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string baseDate = request.Value1;

                List<RetensionResponse> retentiongList = new List<RetensionResponse>();

                DateTime baseDateTime = DateTime.Parse(baseDate);
                long baseCount = await _redisService.GetUserRetention(baseDate, baseDate);

                for (int i = 0; i < 30; i++)
                {
                    string fieldDate = baseDateTime.AddDays(i).ToString("yyyy-MM-dd");
                    long curCount = await _redisService.GetUserRetention(baseDate, fieldDate);

                    float curRate = 0;
                    if (curCount > 0 && baseCount > 0) curRate = (float)curCount / baseCount * 100f;

                    retentiongList.Add(new RetensionResponse { date = fieldDate, count = curCount, rate = curRate });
                }

                return Ok(new { ok = true, retentiondata = retentiongList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }


        public class MarketingResponse
        {
            public string date { get; set; } = "";
            public long totalpayment { get; set; }
            public long marketingprice { get; set; }
            public float marketingrate { get; set; }
            public float paymentrate { get; set; }
            public float arpu { get; set; }
            public float arppu { get; set; }
        }
        [HttpPost("usermarketingdata")]
        public async Task<IActionResult> UserMarketingData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string startDate = request.Value1;
                string endDate = request.Value2;

                if (startDate == "" || endDate == "") return Unauthorized(new { ok = false, message = $"날짜 세팅 필요" });

                int count = (int)(DateTime.Parse(endDate) - DateTime.Parse(startDate)).TotalDays;

                List<MarketingResponse> marketingList = new List<MarketingResponse>();

                for (int i = 0; i <= count; i++)
                {
                    string checkDate = DateTime.Parse(startDate).AddDays(i).ToString("yyyy-MM-dd");

                    HashEntry[] userEntry = await _redisService.HGetAllUserCreateDay(checkDate);

                    long totalUserCount = userEntry.Length;
                    long paymentUserCount = 0;
                    long totalPayment_= 0;
                    for (int j = 0; j < userEntry.Length; j++)
                    {
                        string checkUserId = _redisService.RedisToString(userEntry[j].Name);
                        string userpayment = await _redisService.GetUserIntData(checkUserId, Characterdata_int.usertotalpaymentprice);

                        if (userpayment != "") 
                        {
                            long userpayment_ = long.Parse(userpayment);
                            if (userpayment_ > 0)
                            {
                                paymentUserCount++;
                                totalPayment_ += userpayment_;
                            }
                        }
                    }

                    long marketingprice_ = await _redisService.GetMarketingPrice(checkDate);
                    float marketingrate_ = marketingprice_ > 0 ? (float)totalPayment_ / marketingprice_ * 100f : 0;
                    float paymentrate_ = totalPayment_ > 0 ? (float)paymentUserCount / totalUserCount * 100f : 0;
                    float arpu_ = totalPayment_ > 0 ? totalPayment_ / totalUserCount : 0;
                    float arppu_ = totalPayment_ > 0 ? totalPayment_ / paymentUserCount : 0;

                    marketingList.Add(new MarketingResponse { date = checkDate, totalpayment = totalPayment_, marketingprice = marketingprice_, marketingrate = marketingrate_, paymentrate = paymentrate_, arpu = arpu_, arppu = arppu_ });
                }

                return Ok(new { ok = true, marketingData = marketingList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("setmarketingprice")]
        public async Task<IActionResult> SetMarketingPrice([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string startDate = request.Value1;
                string endDate = request.Value2;
                string marketingPrice = request.Value3;
                string[] marketingPriceData = marketingPrice.Split('*');

                if (startDate == "" || endDate == "") return Unauthorized(new { ok = false, message = $"날짜 세팅 필요" });

                for (int i = 0; i < marketingPriceData.Length; i++)
                {
                    string[] marketingPriceDatas = marketingPriceData[i].Split('#');
                    string date = marketingPriceDatas[0];
                    string price = marketingPriceDatas[1];

                    await _redisService.SetMarketingPrice(date, price);
                }

                int count = (int)(DateTime.Parse(endDate) - DateTime.Parse(startDate)).TotalDays;

                List<MarketingResponse> marketingList = new List<MarketingResponse>();

                for (int i = 0; i <= count; i++)
                {
                    string checkDate = DateTime.Parse(startDate).AddDays(i).ToString("yyyy-MM-dd");

                    HashEntry[] userEntry = await _redisService.HGetAllUserCreateDay(checkDate);

                    long totalUserCount = userEntry.Length;
                    long paymentUserCount = 0;
                    long totalPayment_= 0;
                    for (int j = 0; j < userEntry.Length; j++)
                    {
                        string checkUserId = _redisService.RedisToString(userEntry[j].Name);
                        string userpayment = await _redisService.GetUserIntData(checkUserId, Characterdata_int.usertotalpaymentprice);

                        if (userpayment != "")
                        {
                            long userpayment_ = long.Parse(userpayment);
                            if (userpayment_ > 0)
                            {
                                paymentUserCount++;
                                totalPayment_ += userpayment_;
                            }
                        }
                    }

                    long marketingprice_ = await _redisService.GetMarketingPrice(checkDate);
                    float marketingrate_ = marketingprice_ > 0 ? (float)totalPayment_ / marketingprice_ * 100f : 0;
                    float paymentrate_ = totalPayment_ > 0 ? (float)paymentUserCount / totalUserCount * 100f : 0;
                    float arpu_ = totalPayment_ > 0 ? totalPayment_ / totalUserCount : 0;
                    float arppu_ = totalPayment_ > 0 ? totalPayment_ / paymentUserCount : 0;

                    marketingList.Add(new MarketingResponse { date = checkDate, totalpayment = totalPayment_, marketingprice = marketingprice_, marketingrate = marketingrate_, paymentrate = paymentrate_, arpu = arpu_, arppu = arppu_ });
                }

                return Ok(new { ok = true, marketingData = marketingList });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("settotalintdata")]
        public async Task<IActionResult> SetTotalIntData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string intIndex = request.Value1;
                string intData = request.Value2;

                if (intIndex == "" || intData == "") return Unauthorized(new { ok = false, message = $"데이터 세팅 필요" });

                List<string> totalUserIds = await _redisService.GetAllUserIDs();

                if (intIndex == ((int)Characterdata_int.tutorialindex).ToString()) 
                {
                    for (int i = 0; i < totalUserIds.Count; i++)
                    {
                        string curTutorialIndex = await _redisService.GetUserIntData(totalUserIds[i], (RedisService.Characterdata_int)int.Parse(intIndex));

                        if(curTutorialIndex == "")
                        {

                        }
                        else
                        {
                            if (int.Parse(curTutorialIndex) >= int.Parse(intData) && int.Parse(curTutorialIndex) < (int)TutorialIndex_.Max)
                            {
                                await _redisService.SetUserIntData(totalUserIds[i], (RedisService.Characterdata_int)int.Parse(intIndex), (int.Parse(curTutorialIndex) + 1).ToString());
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < totalUserIds.Count; i++)
                    {
                        await _redisService.SetUserIntData(totalUserIds[i], (RedisService.Characterdata_int)int.Parse(intIndex), intData);
                    }
                }

                return Ok(new { ok = true });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }

        [HttpPost("settotalstringdata")]
        public async Task<IActionResult> SetTotalStringData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string stringIndex = request.Value1;
                string stringData = request.Value2;

                if (stringIndex == "") return Unauthorized(new { ok = false, message = $"데이터 세팅 필요" });

                List<string> totalUserIds = await _redisService.GetAllUserIDs();

                for (int i = 0; i < totalUserIds.Count; i++)
                {
                    await _redisService.SetUserStringData(totalUserIds[i], (RedisService.Characterdata_string)int.Parse(stringIndex), stringData);
                }

                return Ok(new { ok = true });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        public class LastSaveTimeResponse
        {
            public string nickname { get; set; } = "";
            public string createtime { get; set; } = "";
            public string lastsavetime { get; set; } = "";
        }

        public enum PlayingTime_
        {
            day1, 
            hour12, 
            hour6, 
            hour5, 
            hour4, 
            hour3, 
            hour2, 
            hour1, 
            min30,
            min10,
            other,
            max,
        }
        [HttpPost("getuserlastsavetime")]
        public async Task<IActionResult> GetUserLastSaveTime([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string startDate = request.Value1;
                string endDate = request.Value2;

                if (startDate == "" || startDate == "") return Unauthorized(new { ok = false, message = $"날짜 세팅 필요" });

                int count = (int)(DateTime.Parse(endDate) - DateTime.Parse(startDate)).TotalDays;

                List<LastSaveTimeResponse> timeList = new List<LastSaveTimeResponse>();

                int[] playTimeArr = new int[(int)PlayingTime_.max];

                for (int i = 0; i <= count; i++)
                {
                    string checkDate = DateTime.Parse(startDate).AddDays(i).ToString("yyyy-MM-dd");

                    HashEntry[] userIdEntry = await _redisService.HGetAllUserCreateDay(checkDate);

                    for (int j = 0; j < userIdEntry.Length; j++)
                    {
                        string userid = _redisService.RedisToString(userIdEntry[j].Name);

                        string nickname = await _redisService.GetUserStringData(userid, Characterdata_string.nickname);
                        string createTime = await _redisService.GetUserStringData(userid, Characterdata_string.createtime);
                        string lastsavetime = await _redisService.GetUserStringData(userid, Characterdata_string.lastsavetime);

                        if(createTime != "" && lastsavetime != "")
                        {
                            timeList.Add(new LastSaveTimeResponse { nickname = nickname, createtime = createTime, lastsavetime = lastsavetime });

                            int playday = (int)(DateTime.Parse(lastsavetime) - DateTime.Parse(createTime)).TotalDays;
                            int playtime_hour = (int)(DateTime.Parse(lastsavetime) - DateTime.Parse(createTime)).TotalHours;
                            int playtime_min = (int)(DateTime.Parse(lastsavetime) - DateTime.Parse(createTime)).Minutes;

                            if (playday >= 1) playTimeArr[(int)PlayingTime_.day1]++;
                            else if (playtime_hour >= 12) playTimeArr[(int)PlayingTime_.hour12]++;
                            else if (playtime_hour >= 6) playTimeArr[(int)PlayingTime_.hour6]++;
                            else if (playtime_hour >= 5) playTimeArr[(int)PlayingTime_.hour5]++;
                            else if (playtime_hour >= 4) playTimeArr[(int)PlayingTime_.hour4]++;
                            else if (playtime_hour >= 3) playTimeArr[(int)PlayingTime_.hour3]++;
                            else if (playtime_hour >= 2) playTimeArr[(int)PlayingTime_.hour2]++;
                            else if (playtime_hour >= 1) playTimeArr[(int)PlayingTime_.hour1]++;
                            else if (playtime_min >= 30) playTimeArr[(int)PlayingTime_.min30]++;
                            else if (playtime_min >= 10) playTimeArr[(int)PlayingTime_.min10]++;
                            else playTimeArr[(int)PlayingTime_.other]++;
                        }
                            
                    }
                }

                return Ok(new { ok = true, timelistdata = timeList, playTimeData = playTimeArr });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("changeuserpetdata")]
        public async Task<IActionResult> ChangeUserPetData([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string changeAbililtyType = request.Value1;
                string changePetLen = request.Value2;

                if (changeAbililtyType == "" || changePetLen == "" ) return Unauthorized(new { ok = false, message = $"데이터 세팅 필요" });

                List<string> totalUserIds = await _redisService.GetAllUserIDs();

                for (int i = 0; i < totalUserIds.Count; i++)
                {
                    HashEntry[] petInven = await _redisService.GetPetInven(totalUserIds[i]);
                    for (int j = 0; j < petInven.Length; j++)
                    {
                        string invenIndex = _redisService.RedisToString(petInven[j].Name);
                        string petInvenData = _redisService.RedisToString(petInven[j].Value);

                        if (petInvenData != "")
                        {
                            string[] petInvenDatas = petInvenData.Split('#');
                            string index = petInvenDatas[(int)User_Pet.index];
                            string abillityType = petInvenDatas[(int)User_Pet.abilityType];
                            string abillityValue = petInvenDatas[(int)User_Pet.abilityValue];

                            if (abillityType == changeAbililtyType) 
                            {
                                petInvenDatas[(int)User_Pet.abilityValue] = (float.Parse(abillityValue) + int.Parse(changePetLen)).ToString();
                                await _redisService.SetPetInven(totalUserIds[i], invenIndex, string.Join("#", petInvenDatas));
                            }
                        }
                    }
                }

                return Ok(new { ok = true });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("setpostallusercoupon")]
        public async Task<IActionResult> SetPostAllUserCoupon([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                string GetCouponName = request.Value1;
                string SetPostName = request.Value2;

                if (GetCouponName == "" || SetPostName == "" ) return Unauthorized(new { ok = false, message = $"데이터 세팅 필요" });

                string couponDetail = await _redisService.GetCoupon(GetCouponName);

                if (couponDetail == "") return Unauthorized(new { ok = false, message = $"없는 쿠폰 내역" });

                string[] couponDetails = couponDetail.Split('#');
                string reward = couponDetails[(int)Coupon_.reward];
                string[] rewards = reward.Split('^');

                string couponUse = await _redisService.GetCouponUse(GetCouponName);
                List<string> couponUseList = couponUse == "" ? new List<string>() : couponUse.Split('#').ToList();

                List<string> totalUserIds = await _redisService.GetAllUserIDs();

                for (int i = 0; i < totalUserIds.Count; i++)
                {
                    if (couponUseList.Contains(totalUserIds[i]) == false) 
                    {
                        for (int j = 0; j < rewards.Length; j++)
                        {
                            string[] rewards_ = rewards[j].Split('*');
                            string rewardType = rewards_[0];
                            string rewardValue = rewards_[1];

                            await _redisService.ProcessPost(totalUserIds[i], rewardType, rewardValue, SetPostName);
                        }
                    }
                }

                return Ok(new { ok = true });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
        [HttpPost("resetdailyshop")]
        public async Task<IActionResult> ResetDailyShop([FromBody] RedisService.RequestList request)
        {
            try
            {
                if (await _authService.IsValidSession(Request) == false) return Unauthorized(new { ok = false, message = "invalid_session" });

                List<string> totalUserIds = await _redisService.GetAllUserIDs();

                for (int i = 0; i < totalUserIds.Count; i++)
                {
                    if (totalUserIds[i] == "") continue;

                    string dailyshop = await _redisService.GetUserStringData(totalUserIds[i], Characterdata_string.dailyShop);

                    if (dailyshop == "") continue;

                    int dataLen = dailyshop.Split('*').Length;
                  
                    if (dataLen < 10) 
                    {
                        Dictionary<string, string[]> shopDic = await _redisService.GetTemplateAll(TEMPLATE_TYPE.shop);

                        List<string> randomIndexList = new List<string>();
                        List<float> randomPerList = new List<float>();
                        float totalPer = 0;

                        foreach (var data in shopDic)
                        {
                            if (data.Value[(int)shoptemplate_.tab] != ((int)shopTab_.DailyShop).ToString()) continue;

                            string index = data.Value[(int)shoptemplate_.index];
                            string randomper = data.Value[(int)shoptemplate_.randomper];
                            string needtype = data.Value[(int)shoptemplate_.needtype];

                            if (randomper != "")
                            {
                                randomIndexList.Add(index);
                                randomPerList.Add(float.Parse(randomper));
                            }
                        }

                        for (int j = 0; j < randomPerList.Count; j++) totalPer += randomPerList[j];

                        string newDailyShopData = "";

                        for (int j = 0; j < 10 - dataLen; j++) 
                        {
                            float rand = GetRandomFloat(0, totalPer);
                            float cumulative = 0f;

                            for (int k = 0; k < randomPerList.Count; k++)
                            {
                                cumulative += randomPerList[k];
                                if (rand < cumulative)
                                {
                                    float discountPer = GetRandomFloat(0, 100);

                                    if (discountPer < 50) newDailyShopData += $"*{randomIndexList[k]}#30#0";
                                    else newDailyShopData += $"*{randomIndexList[k]}#0#0";

                                    break;
                                }
                            }
                        }

                        await _redisService.SetUserStringData(totalUserIds[i], Characterdata_string.dailyShop, $"{dailyshop}{newDailyShopData}");
                    }
                }
              

                return Ok(new { ok = true });
            }
            catch (Exception ex)
            {
                return Unauthorized(new { ok = false, message = ex.ToString() });
            }
        }
    }
}


