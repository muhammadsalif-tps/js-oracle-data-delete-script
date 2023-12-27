const Helpers = (() => {

  function getRandomDate(startDateStr, endDateStr, targetMonths = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]) {
    let startDate = new Date(startDateStr);
    let endDate = new Date(endDateStr);

    let targetDates = [];
    for (let d = startDate; d <= endDate; d.setDate(d.getDate() + 1)) {
      if (targetMonths.includes(d.getMonth())) {
        targetDates.push(new Date(d));
      }
    }

    let randomDate = targetDates[Math.floor(Math.random() * targetDates.length)];
    let _randomDate = randomDate.toISOString().split('T')[0];
    return _randomDate;
  }

  const calculateTimePassed = (startTime, endTime) => {
    const timeDiff = endTime - startTime;
    const seconds = Math.floor(timeDiff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    const remainingHours = hours % 24;
    const remainingMinutes = minutes % 60;
    const remainingSeconds = seconds % 60;

    const formattedTime = `${days} days, ${remainingHours} hours, ${remainingMinutes} minutes, and ${remainingSeconds} seconds`;

    return formattedTime;
  }
  return { getRandomDate, calculateTimePassed }
})();

module.exports = Helpers;