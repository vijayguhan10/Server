const QRCode = require('qrcode');
const PDFDocument = require('pdfkit');
const fs = require('fs');

const url = 'https://forms.gle/5vYhr8mUByxorjBw9';
const outputPdf = 'qr_code.pdf';

(async () => {
  // Generate QR code as a PNG buffer
  const qrBuffer = await QRCode.toBuffer(url, { type: 'png', width: 300 });

  // Create PDF and embed QR code
  const doc = new PDFDocument();
  doc.pipe(fs.createWriteStream(outputPdf));
  doc.fontSize(16).text('Scan the QR code to open the form:', { align: 'center' });
  doc.moveDown();
  doc.image(qrBuffer, { fit: [200, 200], align: 'center' });
  doc.moveDown();
  doc.fontSize(10).text(url, { align: 'center', underline: true });
  doc.end();

  console.log(`✅ PDF saved as ${outputPdf}`);
})();