const { MongoClient } = require('mongodb');
const client = new MongoClient('mongodb://admin:admin123@localhost:27017/testdb?authSource=admin');

async function run() {
  await client.connect();
  const db = client.db('testdb');

  try { await db.collection('conciliacion').drop(); } catch {}
  const col = db.collection('conciliacion');

  await col.insertMany([
    {
      institutionCode: 4,
      cutOffDate: '2025-05-04',
      conciliationDate: new Date('2025-05-14T10:03:32.007Z'),
      transactionCount: 3,
      transactionError: 2,
      transactionOk: 1,
      pending: 2,
      reconciled: 1,
      debitTotal: 234.54,
      creditTotal: 15,
      netAmount: -219.54,
      creationDate: new Date('2025-05-14T10:03:32.007Z'),
      creationUser: 'admin',
      editDate: new Date('2025-05-14T10:03:32.007Z'),
      editUser: '',
      statusConciliation: 1,
      details: [
        {
          id: 1, state: 1, observation: 'OK', reconciled: 1, description: '',
          uuid: 'b1dec13b-b321-44e9-80a3-90b2d9ec56f0',
          cutOffNumber: '', cutOffDate: '0001-01-01',
          transactionDateSwitch: new Date('2025-05-04T16:50:56.655Z'),
          accountTypeIdSwitch: 'Y9K5kRnyEjLdQpRXS1iKaw==',
          accountNumberSwitch: 'cg2zp/8oKDg8mkr8qnS6oA==',
          transactionValueSwitch: 15,
          movementCodeSwitch: '15624691', transactionTypeSwitch: 'C',
          institutionCodeSwitch: 4, serviceCodeSwitch: 1, transactionStatusSwitch: 1,
          reverseMovementCodeSwitch: '',
          transactionDateInstitution: new Date('2025-05-04T16:50:56.000Z'),
          accountTypeIdInstitution: 'Y9K5kRnyEjLdQpRXS1iKaw==',
          accountNumberInstitution: 'cg2zp/8oKDg8mkr8qnS6oA==',
          transactionValueInstitution: 15,
          movementCodeInstitution: '15624691', transactionTypeInstitution: 'C',
          institutionCodeInstitution: 4, serviceCodInstitution: 'QR',
          transactionStatusInstitution: 1, reverseMovementCodeInstitution: '',
          editDate: new Date('2025-05-14T10:03:32.031Z')
        },
        {
          id: 2, state: 0, observation: 'No existe la transaccion en la institucion', reconciled: 0,
          description: '', uuid: 'b1dec13b-b321-44e9-80a3-90b2d9ec56f0',
          cutOffNumber: '', cutOffDate: '',
          transactionDateSwitch: new Date('2025-05-04T16:50:56.655Z'),
          accountTypeIdSwitch: 'dKog62Hg2Rb8bs+JYuy0zQ==',
          accountNumberSwitch: '3qHuYYMpx18lHSr3Ktsbpw==',
          transactionValueSwitch: 15,
          movementCodeSwitch: '06060606', transactionTypeSwitch: 'D',
          institutionCodeSwitch: 4, serviceCodeSwitch: 1, transactionStatusSwitch: 1,
          reverseMovementCodeSwitch: '',
          transactionDateInstitution: null,
          accountTypeIdInstitution: '', accountNumberInstitution: '',
          transactionValueInstitution: 0,
          movementCodeInstitution: '', transactionTypeInstitution: '',
          institutionCodeInstitution: 0, serviceCodInstitution: '',
          transactionStatusInstitution: 0, reverseMovementCodeInstitution: '',
          editDate: new Date('2025-05-14T10:03:32.031Z')
        },
        {
          id: 4, state: 0, observation: 'Sin coincidencia, no existe registro en el SWITCH', reconciled: 0,
          description: '', uuid: 'f57f4a1c-3331-4337-965c-ada40eadc3a0',
          cutOffNumber: '', cutOffDate: '0001-01-01',
          transactionDateSwitch: null,
          accountTypeIdSwitch: '', accountNumberSwitch: '',
          transactionValueSwitch: 0,
          movementCodeSwitch: '', transactionTypeSwitch: '',
          institutionCodeSwitch: 0, serviceCodeSwitch: '', transactionStatusSwitch: 0,
          reverseMovementCodeSwitch: '',
          transactionDateInstitution: new Date('2025-05-04T16:50:56.000Z'),
          accountTypeIdInstitution: 'dKog62Hg2Rb8bs+JYuy0zQ==',
          accountNumberInstitution: '3qHuYYMpx18lHSr3Ktsbpw==',
          transactionValueInstitution: 234.54,
          movementCodeInstitution: '06060606', transactionTypeInstitution: 'D',
          institutionCodeInstitution: 4, serviceCodInstitution: 'QR',
          transactionStatusInstitution: 1, reverseMovementCodeInstitution: '',
          editDate: new Date('2025-05-14T10:03:32.032Z')
        }
      ]
    },
    {
      institutionCode: 4,
      cutOffDate: '2025-05-05',
      conciliationDate: new Date('2025-05-15T08:20:00.000Z'),
      transactionCount: 5,
      transactionError: 1,
      transactionOk: 4,
      pending: 1,
      reconciled: 4,
      debitTotal: 980.00,
      creditTotal: 750.50,
      netAmount: -229.50,
      creationDate: new Date('2025-05-15T08:20:00.000Z'),
      creationUser: 'admin',
      editDate: new Date('2025-05-15T08:20:00.000Z'),
      editUser: '',
      statusConciliation: 1,
      details: [
        {
          id: 1, state: 1, observation: 'OK', reconciled: 1, description: '',
          uuid: 'a2bc34de-1234-5678-abcd-ef1234567890',
          cutOffNumber: 'C001', cutOffDate: '2025-05-05',
          transactionDateSwitch: new Date('2025-05-05T09:10:00.000Z'),
          accountTypeIdSwitch: 'ABC123==', accountNumberSwitch: 'XYZ789==',
          transactionValueSwitch: 750.50,
          movementCodeSwitch: '20250505001', transactionTypeSwitch: 'C',
          institutionCodeSwitch: 4, serviceCodeSwitch: 1, transactionStatusSwitch: 1,
          reverseMovementCodeSwitch: '',
          transactionDateInstitution: new Date('2025-05-05T09:10:00.000Z'),
          accountTypeIdInstitution: 'ABC123==', accountNumberInstitution: 'XYZ789==',
          transactionValueInstitution: 750.50,
          movementCodeInstitution: '20250505001', transactionTypeInstitution: 'C',
          institutionCodeInstitution: 4, serviceCodInstitution: 'QR',
          transactionStatusInstitution: 1, reverseMovementCodeInstitution: '',
          editDate: new Date('2025-05-15T08:20:00.000Z')
        },
        {
          id: 2, state: 0, observation: 'Monto no coincide entre SWITCH e institucion', reconciled: 0,
          description: 'Diferencia de 10.00', uuid: 'c3de45ef-2345-6789-bcde-ef2345678901',
          cutOffNumber: '', cutOffDate: '2025-05-05',
          transactionDateSwitch: new Date('2025-05-05T11:30:00.000Z'),
          accountTypeIdSwitch: 'DEF456==', accountNumberSwitch: 'UVW012==',
          transactionValueSwitch: 490.00,
          movementCodeSwitch: '20250505002', transactionTypeSwitch: 'D',
          institutionCodeSwitch: 4, serviceCodeSwitch: 1, transactionStatusSwitch: 1,
          reverseMovementCodeSwitch: '',
          transactionDateInstitution: new Date('2025-05-05T11:30:00.000Z'),
          accountTypeIdInstitution: 'DEF456==', accountNumberInstitution: 'UVW012==',
          transactionValueInstitution: 480.00,
          movementCodeInstitution: '20250505002', transactionTypeInstitution: 'D',
          institutionCodeInstitution: 4, serviceCodInstitution: 'QR',
          transactionStatusInstitution: 1, reverseMovementCodeInstitution: '',
          editDate: new Date('2025-05-15T08:20:00.000Z')
        }
      ]
    },
    {
      institutionCode: 7,
      cutOffDate: '2025-05-06',
      conciliationDate: new Date('2025-05-16T14:45:00.000Z'),
      transactionCount: 8,
      transactionError: 3,
      transactionOk: 5,
      pending: 3,
      reconciled: 5,
      debitTotal: 1500.00,
      creditTotal: 1200.00,
      netAmount: -300.00,
      creationDate: new Date('2025-05-16T14:45:00.000Z'),
      creationUser: 'operador1',
      editDate: new Date('2025-05-16T14:45:00.000Z'),
      editUser: '',
      statusConciliation: 0,
      details: [
        {
          id: 1, state: 1, observation: 'OK', reconciled: 1, description: '',
          uuid: 'd4ef56fa-3456-7890-cdef-ef3456789012',
          cutOffNumber: 'C010', cutOffDate: '2025-05-06',
          transactionDateSwitch: new Date('2025-05-06T10:00:00.000Z'),
          accountTypeIdSwitch: 'GHI789==', accountNumberSwitch: 'RST345==',
          transactionValueSwitch: 300.00,
          movementCodeSwitch: '20250506001', transactionTypeSwitch: 'C',
          institutionCodeSwitch: 7, serviceCodeSwitch: 2, transactionStatusSwitch: 1,
          reverseMovementCodeSwitch: '',
          transactionDateInstitution: new Date('2025-05-06T10:00:00.000Z'),
          accountTypeIdInstitution: 'GHI789==', accountNumberInstitution: 'RST345==',
          transactionValueInstitution: 300.00,
          movementCodeInstitution: '20250506001', transactionTypeInstitution: 'C',
          institutionCodeInstitution: 7, serviceCodInstitution: 'TR',
          transactionStatusInstitution: 1, reverseMovementCodeInstitution: '',
          editDate: new Date('2025-05-16T14:45:00.000Z')
        }
      ]
    }
  ]);

  // Create indexes
  await col.createIndex({ institutionCode: 1 });
  await col.createIndex({ cutOffDate: 1 });
  await col.createIndex({ statusConciliation: 1 });
  await col.createIndex({ institutionCode: 1, cutOffDate: 1 }, { unique: true });

  const count = await col.countDocuments();
  console.log('Documentos insertados:', count);

  const sample = await col.find({}, {
    projection: { _id: 1, institutionCode: 1, cutOffDate: 1, transactionCount: 1, reconciled: 1, pending: 1, netAmount: 1, statusConciliation: 1 }
  }).toArray();
  console.log('Resumen:', JSON.stringify(sample, null, 2));
  await client.close();
}

run().catch(e => { console.error('ERROR:', e.message); process.exit(1); });
