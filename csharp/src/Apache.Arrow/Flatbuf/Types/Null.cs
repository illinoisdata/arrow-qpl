// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::System.Collections.Generic;
using global::Google.FlatBuffers;

/// These are stored in the flatbuffer in the Type union below
internal struct Null : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static void ValidateVersion() { FlatBufferConstants.FLATBUFFERS_23_5_9(); }
  public static Null GetRootAsNull(ByteBuffer _bb) { return GetRootAsNull(_bb, new Null()); }
  public static Null GetRootAsNull(ByteBuffer _bb, Null obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p = new Table(_i, _bb); }
  public Null __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }


  public static void StartNull(FlatBufferBuilder builder) { builder.StartTable(0); }
  public static Offset<Null> EndNull(FlatBufferBuilder builder) {
    int o = builder.EndTable();
    return new Offset<Null>(o);
  }
}


static internal class NullVerify
{
  static public bool Verify(Google.FlatBuffers.Verifier verifier, uint tablePos)
  {
    return verifier.VerifyTableStart(tablePos)
      && verifier.VerifyTableEnd(tablePos);
  }
}

}
