Sapphire Plugin for dCache
==================================

This is "**s**mallfile to **a**rchive **p**acking **p**lugin for
**h**igh **i**ngest **re**search" plugin for dCache. It consists
of two parts, the driver for dCache and the (un-)packing part that
runs independent of dCache on another machine.

Using the driver with dCache
----------------------------

To use this plugin with dCache, place the directory containing this
file in /usr/local/share/dcache/plugins/ on a dCache pool. Restart
the pool to load the plugin.

To verify that the plugin is loaded, navigate to the pool in the dCache admin
shell and issue the command:

    hsm show providers

The plugin should be listed as *Sapphire*.

To activate the plugin, create an HSM instance using:

    hsm create osm name Sapphire [-key=value]...
