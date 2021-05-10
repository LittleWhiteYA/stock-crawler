#!/usr/bin/env node

require('@babel/register')({ extensions: ['.js', '.ts'] });

require('dotenv').config();

const { TelegramClient } = require('messaging-api-telegram');
const format = require('date-fns/format');
const pMap = require('p-map');
const delay = require('delay');

const getDatabase = require('./database');
const excludeBigCompanyCode = require('./excludeBigCompanyCode');

const botToken = process.env.BOT_TOKEN;
const userIds = process.env.USER_IDS.split(',');
const env = process.env.NODE_ENV;

const bot = new TelegramClient({
  accessToken: botToken,
});

const formatMessages = (messages) =>
  messages.map((msg) => {
    const title =
      `date: ${format(msg.date, 'yyyy-MM-dd')} ${msg.time}\n` +
      `code: ${msg.company_code}\n` +
      `name: ${msg.company_name.replace('*', '')}\n` +
      `typek: ${msg.typek}\n` +
      `title: ${msg.title}\n`;

    return {
      title,
      option: {
        parse_mode: 'Markdown',
        reply_markup: {
          inline_keyboard: [
            [
              {
                text: `${msg.company_name}`,
                url: msg.url,
              },
            ],
          ],
        },
      },
    };
  });

const main = async () => {
  const db = await getDatabase();

  const today = new Date();
  const todayDate = new Date(
    Date.UTC(today.getFullYear(), today.getMonth(), today.getDate())
  );

  const baseFilter = {
    // date: { $gte: new Date(2021, 0, 1), $lte: new Date(2021, 2, 1) },
    date: { $gte: todayDate },
    company_code: { $nin: excludeBigCompanyCode },
    typek: { $ne: 'rotc' },
  };

  const sellMessages = await db
    .collection('company_daily_messages')
    .find({
      ...baseFilter,
      title: {
        $regex: '((?=.*處分)(?!.*存款)(?!.*理財).*|處置|出售)',
      },
    })
    .sort({ company_code: 1 })
    .toArray();

  const reportMessages = await db
    .collection('company_daily_messages')
    .find({
      ...baseFilter,
      title: {
        $regex: '合併財報|財務報告|減資|股利',
      },
    })
    .sort({ company_code: 1 })
    .toArray();

  const attentionMessages = await db
    .collection('company_daily_messages')
    .find({
      ...baseFilter,
      title: {
        $regex: '注意交易資訊標準',
      },
    })
    .sort({ company_code: 1 })
    .toArray();

  const messages = [
    { title: '處分 or 處置 or 出售' },
    ...formatMessages(sellMessages),
    { title: '合併財報 or 財務報告 or 減資 or 股利' },
    ...formatMessages(reportMessages),
    { title: '注意交易資訊標準' },
    ...formatMessages(attentionMessages),
  ];

  if (messages.length > 0) {
    await pMap(
      messages,
      async (message) => {
        await pMap(userIds, async (userId) => {
          if (env === 'production') {
            await bot.sendMessage(userId, message.title, message.option);
            await delay(3000);
          } else {
            console.log(`send message to user: ${userId}`);
            console.log({
              title: message.title,
              option: message.option,
            });
            await delay(500);
          }
        });
      },
      { concurrency: 1 }
    );
  } else {
    await pMap(userIds, async (userId) => {
      await bot.sendMessage(
        userId,
        `date: ${format(todayDate, 'yyyy-MM-dd')} has no messages`
      );
    });
  }
};

main()
  .catch(console.error)
  .then(() => process.exit(0));
