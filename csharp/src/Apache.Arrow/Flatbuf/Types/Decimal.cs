// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

/// Exact decimal value represented as an integer value in two's
/// complement. Currently only 128-bit (16-byte) and 256-bit (32-byte) integers
/// are used. The representation uses the endianness indicated
/// in the Schema.
internal struct Decimal : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Decimal GetRootAsDecimal(ByteBuffer _bb) { return GetRootAsDecimal(_bb, new Decimal()); }
  public static Decimal GetRootAsDecimal(ByteBuffer _bb, Decimal obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Decimal __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /// Total number of decimal digits
  public int Precision { get { int o = __p.__offset(4); return o != 0 ? __p.bb.GetInt(o + __p.bb_pos) : (int)0; } }
  /// Number of digits after the decimal point "."
  public int Scale { get { int o = __p.__offset(6); return o != 0 ? __p.bb.GetInt(o + __p.bb_pos) : (int)0; } }
  /// Number of bits per value. The only accepted widths are 128 and 256.
  /// We use bitWidth for consistency with Int::bitWidth.
  public int BitWidth { get { int o = __p.__offset(8); return o != 0 ? __p.bb.GetInt(o + __p.bb_pos) : (int)128; } }

  public static Offset<Decimal> CreateDecimal(FlatBufferBuilder builder,
      int precision = 0,
      int scale = 0,
      int bitWidth = 128) {
    builder.StartTable(3);
    Decimal.AddBitWidth(builder, bitWidth);
    Decimal.AddScale(builder, scale);
    Decimal.AddPrecision(builder, precision);
    return Decimal.EndDecimal(builder);
  }

  public static void StartDecimal(FlatBufferBuilder builder) { builder.StartTable(3); }
  public static void AddPrecision(FlatBufferBuilder builder, int precision) { builder.AddInt(0, precision, 0); }
  public static void AddScale(FlatBufferBuilder builder, int scale) { builder.AddInt(1, scale, 0); }
  public static void AddBitWidth(FlatBufferBuilder builder, int bitWidth) { builder.AddInt(2, bitWidth, 128); }
  public static Offset<Decimal> EndDecimal(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<Decimal>(o);
  }
}


static internal class DecimalVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyField(tablePos, 4 /*Precision*/, 4 /*int*/, 4, false)
      && verifier.VerifyField(tablePos, 6 /*Scale*/, 4 /*int*/, 4, false)
      && verifier.VerifyField(tablePos, 8 /*BitWidth*/, 4 /*int*/, 4, false)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
