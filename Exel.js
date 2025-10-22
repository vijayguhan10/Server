const ExcelJS = require("exceljs");

function normalize(str) {
  return (str || "")
    .toString()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

async function findDuplicateTeams(inputFile, outputFile) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(inputFile);
  const sheet = workbook.worksheets[0];

  // Extract headers
  const rawHeaders = sheet.getRow(1).values;
  const headers = rawHeaders
    .map(h => (typeof h === "string" ? normalize(h) : ""))
    .filter(h => h); // remove empty strings

  const getIndex = (name) => {
    const idx = rawHeaders.findIndex(h => typeof h === "string" && normalize(h).includes(name.toLowerCase()));
    if (idx === -1) throw new Error(`Header not found: ${name}`);
    return idx;
  };

  const teamNameIdx = getIndex("team name");
  const problemNoIdx = getIndex("problem statement number");

  // participant fields
  const participantIndexes = [];
  for (let i = 1; i <= 6; i++) {
    participantIndexes.push({
      name: getIndex(`participant ${i} name`),
      roll: getIndex(`roll number of participant ${i}`),
      phone: getIndex(`mobile number of participant ${i}`)
    });
  }

  // Parse teams
  const teams = [];
  sheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) return;
    const teamName = normalize(row.getCell(teamNameIdx).value);
    const problemNo = normalize(row.getCell(problemNoIdx).value);

    const members = participantIndexes.map(idx => ({
      name: normalize(row.getCell(idx.name).value),
      roll: normalize(row.getCell(idx.roll).value),
      phone: normalize(row.getCell(idx.phone).value)
    }));

    teams.push({ teamName, problemNo, members });
  });

  // Compare teams
  const duplicates = [];
  for (let i = 0; i < teams.length; i++) {
    for (let j = i + 1; j < teams.length; j++) {
      const t1 = teams[i];
      const t2 = teams[j];

      const matchByRoll = t1.members.every(m1 =>
        t2.members.some(m2 => m1.roll && m1.roll === m2.roll)
      );
      const matchByPhone = t1.members.every(m1 =>
        t2.members.some(m2 => m1.phone && m1.phone === m2.phone)
      );
      const matchByName = t1.members.every(m1 =>
        t2.members.some(m2 => m1.name && m1.name === m2.name)
      );

      if (matchByRoll || matchByPhone || matchByName) {
        duplicates.push({ team1: t1, team2: t2 });
      }
    }
  }

  // Write neatly formatted duplicates
  const outWb = new ExcelJS.Workbook();
  const dupSheet = outWb.addWorksheet("Duplicates");

  // Header row
  const headerRow = [
    "Team Name 1",
    "Problem No 1",
    "Team Name 2",
    "Problem No 2",
    "Team Leader (Name)"
  ];
  for (let i = 1; i <= 6; i++) {
    headerRow.push(`Name${i}`, `Roll${i}`, `Phone${i}`);
  }
  dupSheet.addRow(headerRow);

  // Add duplicate rows
  duplicates.forEach(({ team1, team2 }) => {
    const row = [
      team1.teamName,
      team1.problemNo,
      team2.teamName,
      team2.problemNo,
      team1.members[0].name // team leader
    ];
    for (let i = 0; i < 6; i++) {
      row.push(team1.members[i]?.name || "");
      row.push(team1.members[i]?.roll || "");
      row.push(team1.members[i]?.phone || "");
    }
    dupSheet.addRow(row);
  });

  // Styling (highlight duplicate rows)
  dupSheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) {
      row.font = { bold: true, color: { argb: "FFFFFF" } };
      row.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "000000" } };
    } else {
      row.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF3CD" } }; // light yellow highlight
    }
  });

  await outWb.xlsx.writeFile(outputFile);
  console.log(`✅ Duplicates written to ${outputFile}`);
}

// Run
findDuplicateTeams("C:\\Projects\\Non-blocking I-O\\SIH 2025 Team Registration (Responses).xlsx", "duplicates.xlsx");
